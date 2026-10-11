<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# CASSANDRA-16360: CRC32 → CRC32C Migration Plan

> Status: Phase 0 benchmarks run on three platforms (§2.2); more runs are
> needed (Haswell/Broadwell x86, a correctly identified AMD run, ARM servers).
> Phase 1 (plumbing) and Phase 2 (internode CRC32C, negotiated per connection
> in the handshake) are implemented on branch `CASSANDRA-16360-crc32c-plan`.
> Phase 3 (native protocol) and Phase 4 (on-disk formats) are not started.

## 0. Ground rules (restated, binding across all phases)

- Never flip a checksum algorithm globally / process-wide. Every choice of
  CRC32 vs CRC32C must be pinned to a versioned artifact the reader already
  knows before it needs the checksum: internode `MessagingService.Version`,
  native `ProtocolVersion`, or on-disk format version (`sstable format
  version`, `CommitLogDescriptor.version`, `HintsDescriptor` version).
- CRC32 and CRC32C must coexist indefinitely. Old artifacts (files, peers,
  clients) must remain readable/joinable forever (files) or for the
  supported upgrade window (peers/clients), without a flag day.
- Rolling upgrades and mixed-version clusters must not produce spurious
  corruption errors. A checksum-algorithm mismatch must always be caught as
  a **version-negotiation** problem, never surfaced as **data corruption**
  — this is the single highest-risk failure mode in this migration and is
  called out per-phase below.

---

## 1. Inventory of existing CRC32/CRC24 usage

Grouped by area. "Gate today" = what, if anything, currently makes the
checksum choice conditional; "None" means it is hardcoded process-wide,
which is exactly the pattern we must not replicate for CRC32C.

### (a) Internode messaging framing

| Location | What | Hardcoded to | Gate today |
|---|---|---|---|
| `src/java/org/apache/cassandra/utils/Crc.java` | CRC24 (`crc24()`, lines ~90-144, naive bit-loop, Koopman poly `0x1974F0B`) **and** CRC32 helpers (`crc32()`, `computeCrc32(...)`, `updateCrc32(...)`, lines 27-88) via thread-local `java.util.zip.CRC32`, primed with magic 4-byte prefix `{0xFA,0x2D,0x55,0xCA}` (lines 38, 58-63) | `java.util.zip.CRC32` directly, no `ChecksumType` | None |
| `FrameEncoderCrc.java` / `FrameDecoderCrc.java` | Frame header CRC24 + payload CRC32 trailer | via `Crc` | Framing choice (see below), not CRC algorithm |
| `FrameEncoderLZ4.java` / `FrameDecoderLZ4.java` | Header CRC24 + CRC32 of compressed payload | via `Crc` | same |
| `FrameDecoderUnprotected.java` | No checksum; reuses `FrameDecoderCrc` header-parsing statics | N/A | N/A |
| `HandshakeProtocol.java` (~line 40-68) | CRC32 over legacy 3-message handshake `Initiate`/`Accept` | `Crc.computeCrc32` | Legacy-vs-modern handshake is auto-detected by peer capability, not CRC-gated |
| `OutboundConnectionSettings.java` `enum Framing` (lines 50-65) | `UNPROTECTED(0)`, `LZ4(1)`, `CRC(2)` — wire-encoded ordinal in handshake flags | N/A | Chosen from config (`internode_compression`/`internode_encryption`), independent of `MessagingService.Version` |
| `OutboundConnectionInitiator.java` (~355-382) / `InboundConnectionInitiator.java` (~472-506) | Switch on `Framing` to pick encoder/decoder | N/A | Framing enum ordinal, decoupled from messaging version negotiation. Phase 2 adds a separate CRC32C request/acknowledgement to the same handshake (§4.1) |
| `MessagingService.java` `enum Version` (lines 215-229, 262-270) | `VERSION_30..VERSION_60`; `current_version` derived via `currentVersion()` (line 312) from `DatabaseDescriptor.getStorageCompatibilityMode().isBefore(N)` | N/A | `isBefore(N)` / `AcceptVersions` min-max negotiation (lines 279-289). Not usable for CRC32C: framing is sent before negotiation completes, and 6.0 and trunk share `VERSION_60` (§4.2) |
| `journal/*` (`EntrySerializer.java`, `OnDiskIndex.java`, `Journal.java`, `Metadata.java`) | Accord/TCM journal integrity, via `Crc.crc32()` | `Crc.java` (same thread-local instance) | None |

**Biggest hardcoded surface**: `Crc.java` has no `ChecksumType`
abstraction at all — every internode framing class calls its static
methods directly.

### (b) Native client protocol

| Location | What | Gate today |
|---|---|---|
| `src/java/org/apache/cassandra/transport/` | Native protocol v5 framing **is implemented**, by reusing the internode codecs: `PipelineConfigurator.java:390,399` and `SimpleClient.java:603,613` use `net.FrameDecoderCrc.create(allocator)` and `net.FrameEncoderCrc.instance`, and `Flusher.java:55` sizes payloads from their header and trailer lengths | Protocol version only. Any change to the `net.FrameEncoderCrc`/`FrameDecoderCrc` CRC32 singletons is also a change to the client protocol, so Phases 1 and 2 must leave them byte-identical. |
| `ProtocolVersion.java` | `enum {V3, V4, V5, V6(6,"v6-beta", true)}` (lines 42-70); `CURRENT = V5` (87); `BETA = Optional.of(V6)` (88); `isBeta()` (136-139) | Beta-flag idiom: `Envelope.java:441` rejects a beta version unless the client sets `Header.Flag.USE_BETA`; mirrored client-side in `Client.java:61`, `SimpleClient.java:167,206`. **This is the idiom to reuse for staging v6/CRC32C framing.** |
| `doc/native_protocol_v5.spec` (lines 84-173) | Documents CRC24 header + CRC32 payload trailer framing — the authoritative on-wire description, implemented by the shared `net.FrameEncoderCrc`/`FrameEncoderLZ4` codecs | N/A |
| `doc/native_protocol_v3.spec`, `doc/native_protocol_v4.spec` | No framing checksum (pre-v5) | N/A |

### (c) On-disk formats

| Location | What | Hardcoded / abstracted | Gate today |
|---|---|---|---|
| `utils/ChecksumType.java` (88 lines) | `enum {ADLER32, CRC32}`, each wraps a `java.util.zip.*` class via `newInstance()`/`update()`; shared `of(...)` helpers use a `FastThreadLocal<Checksum>` | Abstracted — **natural home for `CRC32C`** | None (2 hardcoded values) |
| `CompressedChunkReader.java` (5 call sites) | Chunk checksum verification | `ChecksumType.CRC32.of(...)` — **already parameterizable** | Hardcoded enum value at call site |
| `DataIntegrityMetadata.java` (`ChecksumValidator`, digest streams) | Fully parameterized on `ChecksumType`, default `CRC32` | Abstracted | Constructor default, but caller can already pass any `ChecksumType` |
| `ChecksummedRandomAccessReader.java` | Hardcodes `ChecksumType.CRC32` when constructing validator | Not parameterized at this call site | None |
| `CassandraCompressedStreamReader.java` (line 74) | Passes `ChecksumType.CRC32` into `CompressedInputStream` | Not parameterized | None |
| `CompressedSequentialWriter.java` + `ChecksumWriter.java` | Per-chunk checksum + whole-file digest (`Digest.crc32` file) | Hardcoded `new CRC32()`, **not** via `ChecksumType` | None |
| `CommitLogSegment.java`, `CommitLogDescriptor.java`, `CommitLogSegmentReader.java`, `CommitLog.java`, `CommitLogReader.java` | Sync-marker, header, and per-mutation checksums | Hardcoded `java.util.zip.CRC32` | `CommitLogReader` delegates some logic to `CommitLogFormat.calculateClaimedChecksum(reader, desc.version)` — **the one place commit-log version already threads into checksum computation; good template** |
| `io/sstable/format/Version.java` | Abstract per-feature gates: `hasMetadataChecksum()`, `hasUIntDeletionTime()`, `hasOldBfFormat()`, etc. | N/A | The `hasXxx()` pattern to extend with `hasCrc32c()` |
| `io/sstable/format/big/BigFormat.java` (lines 436-505) | `current_version` from `StorageCompatibilityMode`; two-letter version string; `version.compareTo("na") >= 0` idiom gates features; `correspondingMessagingVersion` cross-references `MessagingService.Version` | N/A | **Canonical idiom to replicate**: `hasCrc32c = version.compareTo("<next-letter>") >= 0` |
| `db/lifecycle/LogRecord.java` | Transaction log record checksums | CRC32 hit, not deeply inspected | None |
| SAI (`BufferedChecksumIndexInput.java`, `IndexFileUtils.java`) | Index integrity | CRC32 hit, not deeply inspected | None |
| Accord (`CheckpointIntervalArrayIndex.java`, `RouteIndexFormat.java`), Paxos (`PaxosBallotTracker.java`) | Integrity checksums | CRC32 hit, not deeply inspected | None |

### (d) Other

| Location | What | Gate today |
|---|---|---|
| `hints/HintsWriter.java`, `EncryptedHintsWriter.java`, `CompressedHintsWriter.java`, `HintsBuffer.java`, `HintsDescriptor.java`, `ChecksummedDataInput.java` (+ Encrypted/Compressed variants), `ChecksumMismatchException.java` | Hardcoded `java.util.zip.CRC32` via `FBUtilities.updateChecksum{Short,Int,Long}` | `HintsDescriptor` has its own version field but it is not observed gating checksum algorithm choice |
| `utils/FBUtilities.java` (lines 1205-1246) | Generic `updateChecksum*(Checksum, ...)` helpers (algorithm-agnostic) plus two `CRC32`-specific overloads (1230, 1246) | N/A — the CRC32-specific overloads need CRC32C counterparts or generalization to `Checksum` |
| `test/microbench/.../ChecksumBench.java` | **Already** benchmarks `ChecksumType.CRC32`, Guava `Hashing.crc32c()`, and `org.xerial.snappy.PureJavaCrc32C`, with commented-out `java.util.zip.CRC32C` sections (lines 109-131) noting "CRC32C is unavailable in Java 8" — this is effectively a prototype of Phase 0's benchmark and confirms the repo already anticipated this exact migration | N/A |
| `ADLER32` in `ChecksumType` | No live callers found anywhere (`grep -r "ChecksumType.ADLER32"` outside the enum is empty) | Dead code — flag for cleanup, out of scope for this ticket but worth a follow-up JIRA |

### Version-gating idioms already in the codebase (to reuse, not reinvent)

1. **`MessagingService.currentVersion()`** (net/MessagingService.java:312):
   `DatabaseDescriptor.getStorageCompatibilityMode().isBefore(5) ? VERSION_40
   : VERSION_60`, combined with per-connection `AcceptVersions` min/max
   negotiation (lines 279-289). Considered for internode CRC32C and
   rejected (§4.2); Phase 2 negotiates in the handshake instead (§4.1).
2. **`BigFormat.java` two-letter version + `compareTo`** (lines 436-505):
   `hasMetadataChecksum = version.compareTo("na") >= 0`, and
   `correspondingMessagingVersion` cross-referencing a `MessagingService.Version`.
   → model for a new `hasCrc32c()` gate on SSTable format version.
3. **`ProtocolVersion.isBeta()`** + `Header.Flag.USE_BETA` opt-in
   (`Envelope.java:441`, `Client.java:61`, `SimpleClient.java:167,206`). →
   model for staging native protocol v6 behind an explicit client opt-in
   before it becomes non-beta.
4. **`CommitLogFormat.calculateClaimedChecksum(reader, desc.version)`** →
   model for commit-log segment format versioning of checksum choice.

---

## 2. Phase 0 — Benchmark baseline (gate before any code change)

**Goal:** produce a defensible, reviewable performance number before writing
a line of production code, and set a numeric pass/fail bar for Phases 2/3.

### 2.1 JMH suite

New file: `test/microbench/org/apache/cassandra/test/microbench/Crc32VsCrc32cBench.java`,
modeled directly on the existing `ChecksumBench.java` (same package,
`@BenchmarkMode(Mode.AverageTime)`, `@OutputTimeUnit(NANOSECONDS)`,
`@State(Scope.Benchmark)`, `FastThreadExecutor` custom JMH executor).

Implementations under comparison:
- `java.util.zip.CRC32` (current production path, via `ChecksumType.CRC32`) — intrinsic on x86/ARM since always.
- `java.util.zip.CRC32C` (JDK 9+, JIT-intrinsified) — the migration target.
  Trunk's minimum JDK is 11 (`build.xml:47`, `java.supported=11,17,21`), so
  the real JDK class is usable unconditionally on trunk; the commented-out
  block in `ChecksumBench.java:109-131` becomes live code here.
- `org.xerial.snappy.PureJavaCrc32C` (already a repo dependency, used in
  `ChecksumBench.java`) as the pure-Java CRC32C fallback, to document the
  regression risk on any branch/JDK where the intrinsic is unavailable.
- Current `Crc.crc24()` cost, isolated, so we know what we're *removing* in
  native protocol v6 framing (not a candidate for replacement — CRC24 is
  simply dropped in the new protocol version, not swapped for a CRC32C
  variant).

Buffer sizes: `31, 131, 517, 2041` bytes (matches CASSANDRA-16360's posted
numbers, `@Param` on `ChecksumBench.java:56`, kept identical for
comparability) plus frame-realistic sizes `16384` (default frame payload)
and `131072` (128KB).

Heap vs direct buffers: two `@Param` axes — `ByteBuffer.allocate(n)` vs
`ByteBuffer.allocateDirect(n)` — since the framing path
(`FrameEncoderCrc`/`FrameEncoderLZ4`) always uses direct buffers and the
JIT intrinsic has separate fast paths per buffer kind.

**Cold-cache variant** (addresses Benedict's caveat on the ticket that
tight-loop JMH flatters table-based algorithms because the lookup table
stays L2-resident): add a benchmark state that allocates and cycles through
a working set of buffers sized larger than the target CPU's L2 cache (e.g.
16 buffers of 2MB touched round-robin, `@Param` configurable) between
invocations, forcing a cache miss on the checksum table for table-based
implementations. Report this as a separate row, not blended with the hot-loop
numbers.

Architectures: run on x86_64 (SSE4.2 CRC32C instruction) and, if available,
ARM64 (Graviton or Apple Silicon under Rosetta-free JDK) — intrinsic
availability and speed differ per architecture (ARM has a CRC32C
instruction in the ARMv8 crypto extensions; x86 uses `crc32` SSE4.2).

**Deliverable:** a results table (ns/op, GB/s) committed into this plan
(§2.2 placeholder below, filled in once run) plus a **pass/fail gate**:
CRC32C intrinsic must exceed CRC32 intrinsic throughput by a stated margin
(recommend ≥ 20%, matching publicly reported CRC32C hardware speedups) at
16KB and 128KB on the target JDK, on at least one of x86_64/ARM64, before
Phase 2 or Phase 3 implementation begins.

### 2.2 Baseline results table (RUN 1 — ARM64 / Apple M1)

> **This sweep must be repeated on every JDK version and CPU architecture
> available before the Phase 0 gate is treated as decided one way or the
> other.** Only one (JDK, arch) combination has been run so far (JDK 21 /
> arm64, below), and its result — no meaningful CRC32C advantage — is
> plausibly specific to ARM64's CRC extension accelerating both polynomials
> in hardware (§ gate verdict below). At minimum, run the three JMH classes
> (`Crc32VsCrc32cBench`, `Crc32ColdCacheBench`, `Crc24Bench`) on:
> - JDK 11, 17, and 21 (the versions this project actually supports per
>   branch, §7) — CRC32C intrinsic behavior can differ across JDK releases
>   even on the same hardware;
> - `x86_64` (SSE4.2) — the architecture the ticket's original motivation
>   and posted numbers are based on, and which this plan has **not yet
>   tested at all**;
> - `arm64` again on different CPU models (Graviton, other Apple Silicon
>   generations) if available, since ARM CRC extension implementations
>   aren't guaranteed uniform across vendors/generations.
>
> **Append a new dated subsection below (§2.2 RUN 2, RUN 3, ...) for each
> additional (JDK, arch) combination run — do not overwrite RUN 1.** Each
> subsection should follow the same format: an **Environment** line (JDK
> version, OS, CPU model/arch, build/run commands — copy §2.3 verbatim if
> unchanged), the same results table shape, and a gate verdict statement.
> Once at least one x86_64 run exists, add a summary row/table comparing
> the gate verdict across all recorded platforms before using this section
> as evidence for any Phase 2/3 go decision (per §4.7's "not before an
> x86_64 Phase 0 run justifies it").

**Environment:** JDK 21.0.11 (Temurin), macOS 26.5.2, Apple M1 (arm64), `ant build-test`
compiled classes, run via `java -cp ... org.openjdk.jmh.Main`, default class-level
JMH settings (`@Warmup(3, 1s)`, `@Measurement(5, 2s)`, 1 fork, `Threads(4)`).
Raw CSVs: `crc32_vs_crc32c.csv` (60 combinations), `crc32_coldcache.csv`,
`crc24.csv` — generated by the exact commands in §2.3 below, not committed to
the repo (regenerate on demand; the numbers below are transcribed from them).

GB/s computed as `bufferSize / ns_per_op` (1 byte/ns = 1 GB/s).

| Impl | Buffer | Size | Heap ns/op | Direct ns/op | Heap GB/s | Direct GB/s |
|---|---|---|---|---|---|---|
| CRC32 (intrinsic) | hot | 31B | 7.15 | 7.83 | 4.33 | 3.96 |
| CRC32C (intrinsic) | hot | 31B | 7.84 | 7.53 | 3.95 | 4.12 |
| CRC32 (intrinsic) | hot | 131B | 10.63 | 11.79 | 12.32 | 11.11 |
| CRC32C (intrinsic) | hot | 131B | 17.05 | 10.87 | 7.68 | 12.05 |
| CRC32 (intrinsic) | hot | 517B | 47.80 | 47.28 | 10.82 | 10.94 |
| CRC32C (intrinsic) | hot | 517B | 45.97 | 45.98 | 11.25 | 11.24 |
| CRC32 (intrinsic) | hot | 2041B | 245.96 | 243.58 | 8.30 | 8.38 |
| CRC32C (intrinsic) | hot | 2041B | 240.07 | 239.94 | 8.50 | 8.51 |
| CRC32 (intrinsic) | hot | 16KB | 2044.45 | 2062.94 | 8.01 | 7.94 |
| CRC32C (intrinsic) | hot | 16KB | 2037.46 | 2041.02 | 8.04 | 8.03 |
| CRC32 (intrinsic) | hot | 128KB | 16526.01 | 16505.49 | 7.93 | 7.94 |
| CRC32C (intrinsic) | hot | 128KB | 16454.47 | 16473.56 | 7.97 | 7.96 |
| CRC32C (PureJavaCrc32C fallback) | hot | 16KB | 10196.29 | 13055.52 | 1.61 | 1.25 |
| CRC32C (PureJavaCrc32C fallback) | hot | 128KB | 83628.83 | 100668.60 | 1.57 | 1.30 |
| CRC32 (intrinsic) | cold-cache (32MB working set) | 16KB | 2079.93 | (direct-only variant) | 7.88 | — |
| CRC32C (intrinsic) | cold-cache (32MB working set) | 16KB | 2181.72 ± 852 (noisy) | (direct-only variant) | ~7.51 (noisy) | — |
| CRC24 (isolated, 3-byte header) | hot | 3B | 12.71 | n/a | n/a | n/a |
| CRC24 (isolated, 5-byte header) | hot | 5B | 26.97 | n/a | n/a | n/a |

**Gate verdict on this hardware: FAIL.** At the frame-realistic sizes the gate
cares about (16KB and 128KB), CRC32C intrinsic beats CRC32 intrinsic by only
**~0.2–1.1%**, nowhere near the ≥20% margin the gate requires. At small sizes
(31B/131B) the two are within noise of each other, and at 31B HEAP CRC32C is
actually ~9% *slower* than CRC32. The cold-cache variant shows no clear
separation either (within the ±852ns error bar on the CRC32C sample, the two
are statistically indistinguishable).

**Why, and what this means:** this is very likely an **architectural
difference, not a benchmark bug**. x86's SSE4.2 gives hardware acceleration
only to the Castagnoli (CRC32C) polynomial via the `crc32` instruction, so
plain CRC-32 (IEEE 802.3, what `java.util.zip.CRC32` computes) has
historically relied on slower table-based or software-SIMD-folding fallbacks
on x86 — that gap is what CASSANDRA-16360 is about. **ARMv8's CRC extension,
however, provides dedicated hardware instructions for *both* polynomials**
(`crc32b/h/w/x` for IEEE CRC-32 and `crc32cb/h/w/x` for Castagnoli CRC32C),
so on Apple M1 there is close to no hardware asymmetry to exploit — both
checksums are already similarly fast. This was confirmed as a real toggle,
not a no-op flag, via `-XX:+PrintFlagsFinal`: both `UseCRC32Intrinsics` and
`UseCRC32CIntrinsics` default to `true` on this JDK/arch and produce large,
expected slowdowns when disabled at small/medium sizes (see the `-NoIntrinsic`
columns in the raw CSV, e.g. CRC32 HEAP 31B goes from 7.15ns to 120.87ns when
disabled — a real ~17x effect).

**Anomaly flagged, not resolved:** the `-NoIntrinsic` CSV rows show CRC32
(not CRC32C) becoming *faster* than its own intrinsic-enabled run at the
128KB size specifically (HEAP: 6419ns disabled vs 16526ns enabled; DIRECT:
6567ns disabled vs 16505ns enabled) — physically implausible for a flag that
only removes acceleration. This looks like a JIT/native-fallback interaction
specific to large-buffer `CRC32.update(byte[])` (possibly a JNI call into a
platform zlib that has its own independent vectorization, outside the JVM
flag's control) rather than a real regression, but it has **not been
root-caused** and should not be trusted without a profiler pass (async-profiler
flame graph, as the plan already calls for in end-to-end benchmarks). It does
**not** affect the gate verdict above, which only compares the two
intrinsic-enabled (real production) paths — the `-NoIntrinsic` runs are
diagnostic-only and were never the basis for the gate.

**Action required before Phase 2/3 proceed:** re-run this exact suite (§2.3
gives the commands) on x86_64 hardware with SSE4.2 (the architecture the
ticket's original numbers and rationale are based on) before treating the
gate as passed anywhere. This ARM64 run should be read as "the CRC32C
migration case is weak-to-nonexistent on Apple Silicon / current-generation
ARM server CPUs with a full CRC32 hardware instruction," not as a verdict on
x86 Cassandra deployments, which remain the ticket's primary motivation.
Until an x86 run is available, Phase 2/3 should not be signed off on the
performance case alone — only on x86 evidence, per the ground rules'
"never flip a checksum globally" spirit applied to the decision-making
process itself: don't generalize a single architecture's benchmark into a
global rollout decision.

### 2.2 Baseline results table (RUN 2 — x86_64 / Intel Xeon E5-2650 v2, Ivy Bridge EP, 2026-07-19)

**Environment:** JDK 21.0.11 (Ubuntu build 21.0.11+10-1-24.04.2), Ubuntu 24.04
(kernel 6.8.0-60-generic), Intel Xeon E5-2650 v2 @ 2.60GHz (Ivy Bridge EP,
x86_64: SSE4.2 + `pclmulqdq`, AVX but **no AVX2 / AVX-512 / `vpclmulqdq`**),
KVM guest with 4 vCPU (1 thread/core, matching `@Threads(4)`) / 15GB RAM
(Rackspace Spot `gp.vs1.large`, us-central-dfw-1). Same test classes as RUN 1
(Java 21 bytecode from `ant build-test`), same per-class JMH settings; the §2.3
sweep was split into one `org.openjdk.jmh.Main` invocation per benchmark method
with separate CSVs so spot-instance preemptions only cost the chunk in flight
(parameters otherwise identical). Raw CSVs not committed (same policy as RUN 1).

GB/s computed as `bufferSize / ns_per_op` (1 byte/ns = 1 GB/s).

| Impl | Buffer | Size | Heap ns/op | Direct ns/op | Heap GB/s | Direct GB/s |
|---|---|---|---|---|---|---|
| CRC32 (intrinsic) | hot | 31B | 83.46 | 83.38 | 0.37 | 0.37 |
| CRC32C (intrinsic) | hot | 31B | 17.68 | 18.42 | 1.75 | 1.68 |
| CRC32 (intrinsic) | hot | 131B | 96.82 | 104.16 | 1.35 | 1.26 |
| CRC32C (intrinsic) | hot | 131B | 23.75 | 23.79 | 5.52 | 5.51 |
| CRC32 (intrinsic) | hot | 517B | 311.81 | 268.37 | 1.66 | 1.93 |
| CRC32C (intrinsic) | hot | 517B | 58.57 | 65.60 | 8.83 | 7.88 |
| CRC32 (intrinsic) | hot | 2041B | 918.87 | 1129.99 | 2.22 | 1.81 |
| CRC32C (intrinsic) | hot | 2041B | 134.48 | 135.26 | 15.18 | 15.09 |
| CRC32 (intrinsic) | hot | 16KB | 7068.08 | 7061.96 | 2.32 | 2.32 |
| CRC32C (intrinsic) | hot | 16KB | 1068.49 | 977.55 | 15.33 | 16.76 |
| CRC32 (intrinsic) | hot | 128KB | 56242.93 | 55853.97 | 2.33 | 2.35 |
| CRC32C (intrinsic) | hot | 128KB | 7130.55 | 7067.11 | 18.38 | 18.55 |
| CRC32C (PureJavaCrc32C fallback) | hot | 16KB | 14848.80 | 22793.23 | 1.10 | 0.72 |
| CRC32C (PureJavaCrc32C fallback) | hot | 128KB | 116578.30 | 180807.28 | 1.12 | 0.72 |
| CRC32 (intrinsic) | cold-cache (32MB working set) | 16KB | 7679.09 ± 3744 (noisy) | (direct-only variant) | ~2.13 | — |
| CRC32C (intrinsic) | cold-cache (32MB working set) | 16KB | 1183.44 ± 224 | (direct-only variant) | 13.84 | — |
| CRC24 (isolated, 3-byte header) | hot | 3B | 37.58 | n/a | n/a | n/a |
| CRC24 (isolated, 5-byte header) | hot | 5B | 66.82 | n/a | n/a | n/a |

**Gate verdict on this hardware: PASS, overwhelmingly.** At the gate sizes,
CRC32C intrinsic beats CRC32 intrinsic by **+561% / +622%** (16KB HEAP/DIRECT)
and **+689% / +690%** (128KB HEAP/DIRECT) — 6.6–7.9× the throughput, against a
≥20% requirement. CRC32 plateaus at ~2.3 GB/s while CRC32C reaches 15–18.5
GB/s, and the gap holds in the cold-cache variant (7679 ± 3744 vs 1183 ± 224
ns/op) and is already decisive at 131B+. This is precisely the hardware
profile CASSANDRA-16360's original numbers were posted from: SSE4.2's `crc32`
instruction accelerates only the Castagnoli polynomial, and without
AVX2/`vpclmulqdq` the JDK's IEEE CRC-32 path has no fast lane. Sanity checks:
disabling intrinsics slows both paths as expected (CRC32 56243 → 65888 ns,
CRC32C 7131 → 116928 ns at 128KB HEAP), so RUN 1's M1-specific 128KB
no-intrinsic anomaly does **not** reproduce on x86_64.

**Why CRC32 is so slow here, and what is still unverified.** The JDK's x86 CRC32
intrinsic folds with `pclmulqdq`. On Ivy Bridge that instruction has a measured
reciprocal throughput of 8 cycles, against 2 on Haswell and 1 on Skylake
([uops.info](https://uops.info/html-instr/PCLMULQDQ_XMM_XMM_I8.html)). That
explains both the ~2.3 GB/s CRC32 plateau and why disabling the intrinsic costs
only 17%: the folding path is barely faster than the table fallback on this
core. Two things are not explained or recorded:
- CRC32 at 31B takes 83 ns, about 10× RUN 1 and RUN 3. Short buffers take the
  intrinsic's byte-at-a-time tail, but that does not obviously account for 10×.
- Unlike RUN 1, this run did not record `-XX:+PrintFlagsFinal` or the guest's
  CPU flags (§2.3), so it is not proven that the guest exposed `pclmulqdq` and
  that `UseCRC32Intrinsics` was on. Re-run with those recorded before quoting
  RUN 2 upstream.

### 2.2 Baseline results table (RUN 3 — x86_64 / AMD EPYC Milan (Zen 3), 2026-07-19)

**Environment:** identical JDK/OS/harness to RUN 2 (JDK 21.0.11, Ubuntu 24.04,
kernel 6.8.0-60-generic, chunked §2.3 sweep), AMD EPYC-Milan Processor (Zen 3,
x86_64: SSE4.2, `pclmulqdq` **and** `vpclmulqdq`, AVX2, AVX-512), KVM guest
with 4 vCPU (1 thread/core) / 8GB RAM (Rackspace Spot `ch.vs2.large`,
us-central-dfw-2).

| Impl | Buffer | Size | Heap ns/op | Direct ns/op | Heap GB/s | Direct GB/s |
|---|---|---|---|---|---|---|
| CRC32 (intrinsic) | hot | 31B | 8.87 | 9.42 | 3.49 | 3.29 |
| CRC32C (intrinsic) | hot | 31B | 7.27 | 7.21 | 4.26 | 4.30 |
| CRC32 (intrinsic) | hot | 131B | 14.95 | 14.89 | 8.76 | 8.80 |
| CRC32C (intrinsic) | hot | 131B | 10.52 | 10.46 | 12.45 | 12.53 |
| CRC32 (intrinsic) | hot | 517B | 18.38 | 20.83 | 28.13 | 24.82 |
| CRC32C (intrinsic) | hot | 517B | 17.32 | 17.31 | 29.86 | 29.88 |
| CRC32 (intrinsic) | hot | 2041B | 70.88 | 71.17 | 28.80 | 28.68 |
| CRC32C (intrinsic) | hot | 2041B | 70.27 | 70.16 | 29.05 | 29.09 |
| CRC32 (intrinsic) | hot | 16KB | 299.03 | 296.60 | 54.79 | 55.24 |
| CRC32C (intrinsic) | hot | 16KB | 296.72 | 297.54 | 55.22 | 55.06 |
| CRC32 (intrinsic) | hot | 128KB | 2311.16 | 2322.67 | 56.71 | 56.43 |
| CRC32C (intrinsic) | hot | 128KB | 2299.74 | 2302.62 | 56.99 | 56.92 |
| CRC32C (PureJavaCrc32C fallback) | hot | 16KB | 9946.01 | 14256.36 | 1.65 | 1.15 |
| CRC32C (PureJavaCrc32C fallback) | hot | 128KB | 79349.85 | 98591.40 | 1.65 | 1.33 |
| CRC32 (intrinsic) | cold-cache (32MB working set) | 16KB | 449.94 ± 6 | (direct-only variant) | 36.41 | — |
| CRC32C (intrinsic) | cold-cache (32MB working set) | 16KB | 429.86 ± 4 | (direct-only variant) | 38.11 | — |
| CRC24 (isolated, 3-byte header) | hot | 3B | 10.86 | n/a | n/a | n/a |
| CRC24 (isolated, 5-byte header) | hot | 5B | 24.69 | n/a | n/a | n/a |

**Gate verdict on this hardware: FAIL.** Margins at the gate sizes are
**+0.8% / −0.3%** (16KB HEAP/DIRECT) and **+0.5% / +0.9%** (128KB) —
statistical parity, the same picture as Apple M1. Both paths sustain ~55–57
GB/s, about 16 bytes per cycle at Zen 3 clocks.

**The CPU identification for this run needs re-checking before it is quoted.**
Zen 3 has no AVX-512, so the environment line above is wrong about either the
CPU or its flags. And ~16 bytes per cycle is more than either scalar path can
reach: the 128-bit `pclmulqdq` fold measures at a reciprocal throughput of 2
cycles on Zen 3
([uops.info](https://uops.info/html-instr/PCLMULQDQ_XMM_XMM_I8.html)), and the
`crc32` instruction handles 8 bytes at a time. Both numbers point to a wider
vector path (`vpclmulqdq` on 256- or 512-bit registers) being in use for both
algorithms. That fits a Zen 4 host better than Zen 3. Re-run with the guest's
`/proc/cpuinfo` model name and flags and `-XX:+PrintFlagsFinal` recorded
(§2.3). Parity itself is unsurprising on any CPU with a vector carry-less
multiply.
Cold-cache agrees (449.94 ± 6 vs 429.86 ± 4 ns/op, ~4.5% CRC32C). Intrinsic
toggles behave as expected (CRC32 2311 → 30121 ns, CRC32C 2300 → 74544 ns at
128KB HEAP disabled); the M1 anomaly does not reproduce here either.

### 2.2 Gate verdict summary across recorded platforms

| Run | CPU (arch / relevant ISA) | JDK | 16KB margin H/D | 128KB margin H/D | Gate (≥20%) |
|---|---|---|---|---|---|
| RUN 1 | Apple M1 (arm64, ARMv8 CRC ext: both polynomials in HW) | 21.0.11 Temurin | +0.3% / +1.1% | +0.4% / +0.2% | FAIL |
| RUN 2 | Intel Xeon E5-2650 v2 (x86_64 Ivy Bridge, SSE4.2 only) | 21.0.11 Ubuntu | **+561% / +622%** | **+689% / +690%** | **PASS** |
| RUN 3 | Reported as AMD EPYC Milan (Zen 3); CPU identification inconsistent, see RUN 3 | 21.0.11 Ubuntu | +0.8% / −0.3% | +0.5% / +0.9% | FAIL |

**Reading for Phase 2/3 (as required by §4.7):** the gate — "≥20% at 16KB and
128KB … on at least one of x86_64/ARM64" — is now formally **PASSED**, via
RUN 2. The three runs together sharpen the claim: the CRC32C advantage is a
property of x86_64 cores with slow `pclmulqdq`, which per the throughput
figures above means Sandy Bridge and Ivy Bridge (2011–2013), while
current-generation CPUs (the RUN 3 AMD x86_64, Apple M1 arm64) show parity within
±1%. Haswell through Skylake-era Xeons, still common in on-premises fleets,
are untested. A per-cycle model fits RUN 2: eight 128-bit `pclmulqdq` per 64
bytes at 8 cycles each gives 1 byte per cycle, 2.6 GB/s at the nominal 2.6
GHz, against 2.3 GB/s measured; CRC32C ran at about 7 bytes per cycle on the
same machine. Carried forward, and as an inference only, Haswell and
Broadwell (`pclmulqdq` at 2 cycles) would reach about 4 bytes per cycle for
CRC32, so CRC32C would still be **roughly 2× faster**, and the gap would close
from Skylake (1 cycle) on. Nowhere measured is CRC32C *slower* by
more than noise (worst case −0.3%, 16KB DIRECT on Zen 3). Be precise about
what "PASSED" therefore does and does not mean: the gate's "at least one
platform" clause was written when the working assumption was that the CRC32C
win generalized across hardware; the evidence shows it does not. The honest
project-level case is **"opt-in benefit for x86 CPUs with slow carry-less
multiply, measured no-penalty everywhere else"**, not "CRC32C is faster."
That is sufficient to justify Phase 2 as an **opt-in** feature with a
`crc32` default (§4.7), and insufficient, on present evidence, to justify
flipping the default or investing in Phase 3/4 on performance grounds alone.
Caveat: JDK-8-era branches must still skip the software fallback (PureJavaCrc32C
confirmed at only 1.1–1.65 GB/s on both x86 machines, i.e. 2–14× *slower*
than intrinsic CRC32). All three runs used JDK 21.0.11; the §2.2 preamble's
asks for JDK 11/17 datapoints, a Haswell- or Broadwell-era x86_64 run, and a
re-run of RUN 3 with the CPU properly identified remain open. JDK 11 may
differ from JDK 21 on CPUs with AVX-512, because the wider CRC paths are not
in every JDK release (not verified here).

### 2.3 Exact commands to reproduce or extend this run

```
ant build-test   # compiles the 3 new classes under test/microbench

CP="build/apache-cassandra-7.0-SNAPSHOT.jar:build/test/classes"
CP="$CP:$(find build/lib/jars -name '*.jar' | tr '\n' ':')"
CP="$CP:$(find build/test/lib/jars -name '*.jar' | tr '\n' ':')"

# full sweep, default class-level JMH settings, CSV output
java -cp "$CP" org.openjdk.jmh.Main "Crc32VsCrc32cBench" -rf csv -rff crc32_vs_crc32c.csv
java -cp "$CP" org.openjdk.jmh.Main "Crc32ColdCacheBench" -rf csv -rff crc32_coldcache.csv
java -cp "$CP" org.openjdk.jmh.Main "Crc24Bench" -rf csv -rff crc24.csv
```

Record the JDK version, CPU model and JVM flags with every run (see §2.4). On
each new platform, also record the intrinsic flags, so a slow row can be told
apart from a disabled intrinsic:

```
java -XX:+PrintFlagsFinal -version | grep -E 'UseCRC32(C)?Intrinsics|UseCLMUL'
grep -o -w -E 'sse4_2|pclmulqdq|vpclmulqdq|avx2|avx512f' /proc/cpuinfo | sort -u   # x86_64 Linux
```

### 2.4 End-to-end benchmarks (repeated after each phase lands)

- **Internode**: existing messaging microbenchmarks (`MessageOutBench.java`
  as a structural template) extended to report CPU share, plus a two-node
  in-JVM or real-cluster test measuring frame encode/decode CPU via
  async-profiler flame graphs, comparing checksum-attributable cycles
  before vs after CRC32C is negotiated.
- **Client protocol**: `cassandra-stress` mixed read/write workload over
  protocol v5 (CRC32/CRC24) vs v6 (CRC32C), reporting throughput, p99
  latency, coordinator CPU.
- Every result recorded with JDK version, CPU model (`lscpu`/`sysctl -n
  machdep.cpu.brand_string`), and JVM flags, so Phase 0 / Phase 2 / Phase 3
  numbers stay comparable over time.

**Non-goal for Phase 0:** no production code changes. This phase only adds
a new test-scope JMH class.

---

## 3. Phase 1 — Plumbing (no behavior change)

> **Status: implemented** (branch `CASSANDRA-16360-crc32c-plan`). Final scope
> differed from the table below in a few places, discovered during
> implementation:
> - `utils/Crc.java` itself was **not** touched — `FrameEncoderCrc`/
>   `FrameDecoderCrc`/LZ4 variants turned out to be `@ChannelHandler.Sharable`
>   singletons (`FrameEncoderCrc.instance`, `FrameEncoderLZ4.fastInstance`),
>   not per-connection instances, so parameterizing them meant adding
>   additive `getInstance(ChecksumType)`/`create(allocator, ChecksumType)`
>   factory methods alongside the existing singletons/factories (unchanged),
>   with non-CRC32 selection throwing `UnsupportedOperationException` until
>   Phase 2 decides the CRC32C priming strategy. `Crc.java`'s CRC32-specific
>   priming (magic prefix bytes) is deliberately left alone rather than
>   half-generalized.
> - `streaming/CassandraCompressedStreamReader.java` needed no change — it
>   already called `ChecksumType.CRC32` explicitly (see §1(c)), so it was
>   already "plumbed," just not yet wired to anything other than CRC32.
> - `hints/ChecksummedDataInput.java` (the read-side counterpart to the
>   hints writer changes) was added to scope after an initial pass missed it
>   during code review (see below).
> - Every public/protected signature that would otherwise have been widened
>   from concrete `CRC32` to `Checksum` in place (a **binary-incompatible**
>   change for any external caller compiled against the old descriptor) was
>   instead given an additive, `@Deprecated(since="7.0")` `CRC32`-typed
>   (originally tagged `since="5.1"`; corrected in review, since trunk's
>   `base.version` is 7.0 and the tag must name the release that first
>   ships the deprecation)
>   overload delegating to the new `Checksum`-typed one:
>   `FBUtilities.updateChecksum` (×2), `CompressedHintsWriter`'s public
>   constructor, `EncryptedHintsWriter`'s protected constructor (its class is
>   public, so an external subclass in another package could in principle
>   exist). Signatures that were private, or whose enclosing class was not
>   `public` (`CommitLogReader.CommitLogFormat` is a private nested class;
>   `HintsWriter` itself has no `public` modifier), were changed in place
>   with no compatibility shim, since no external `.class` file could
>   possibly hold a reference to them.
> - Verified via `ant build-test` (clean compile) and 8 existing unit test
>   suites across commit log, hints, and compressed-writer paths (all pass
>   unmodified, confirming byte-identical behavior).
> - A third review pass (2026-08-11) found one further binary-compatibility
>   gap the above missed: giving `FrameEncoderCrc` a private
>   `ChecksumType`-taking constructor silently *removed* the implicit public
>   no-arg constructor the class had before. A `@Deprecated(since="7.0")`
>   no-arg constructor delegating to CRC32 was restored. The same pass added
>   known-answer trailer tests to `FramingTest`: the CRC32C frame trailer for
>   the standard `"123456789"` input must equal the published Castagnoli
>   check value `0xE3069283` (pinning "unprimed"), and the CRC32 trailer must
>   equal a plain `java.util.zip.CRC32` fed the `Crc` magic prefix then the
>   payload (pinning the historical priming byte-for-byte, independently of
>   `Crc.java`'s implementation).

**Goal:** make every framing/checksum code path accept a `ChecksumType`
(or equivalent enum) parameter instead of hardcoding CRC32, with zero
observable behavior change. This phase is a pure refactor; it must produce
byte-identical wire/disk output to today.

### 3.1 Files to change

| File | Change |
|---|---|
| `utils/ChecksumType.java` | Add `CRC32C` enum value wrapping `java.util.zip.CRC32C` (guarded appropriately per §6 branch table — see Phase 1 non-goal below for branches lacking JDK 9+). No caller wired to it yet. |
| `utils/Crc.java` | Generalize `crc32()`/`computeCrc32(...)`/`updateCrc32(...)` to accept a `ChecksumType` (or split into `Crc32Util`-style parameterized helpers) while preserving the existing magic-prefix priming behavior for CRC32 exactly as-is. Add parallel CRC32C variants that do **not** reuse the CRC32 priming magic bytes (priming is CRC32-specific tuning, unproven for CRC32C — needs its own analysis, tracked as an open question for Phase 2, not decided in Phase 1). CRC24 code is untouched — it is only removed in Phase 3, never modified in Phase 1/2. |
| `net/FrameEncoderCrc.java`, `net/FrameDecoderCrc.java` | Accept a `ChecksumType` in constructor/factory; internal logic keys off it instead of calling `Crc.crc32()` directly. Default remains hardcoded `ChecksumType.CRC32` at all existing call sites — no behavior change. |
| `net/FrameEncoderLZ4.java`, `net/FrameDecoderLZ4.java` | Same parameterization for the payload CRC32 (header CRC24 stays as CRC24; only the payload checksum is parameterized, since CRC24 is not migrated in Phase 1/2). |
| `net/OutboundConnectionInitiator.java`, `net/InboundConnectionInitiator.java` | No behavior change; continue passing `ChecksumType.CRC32` explicitly at the point where `FrameEncoderCrc`/`FrameDecoderCrc` are constructed, in preparation for Phase 2 wiring a negotiated value here instead. |
| `io/util/DataIntegrityMetadata.java` | Already parameterized — no change needed, just confirm default remains `CRC32`. |
| `io/compress/CompressedSequentialWriter.java`, `io/util/ChecksumWriter.java` | Parameterize on `ChecksumType` instead of `new CRC32()` directly; default `CRC32` preserved. |
| `io/util/ChecksummedRandomAccessReader.java` | Accept `ChecksumType` parameter instead of hardcoding `CRC32`; default preserved at call sites. |
| `streaming/CassandraCompressedStreamReader.java` | Accept `ChecksumType` parameter; default preserved. |
| `db/commitlog/CommitLogDescriptor.java`, `CommitLogSegment.java`, `CommitLogSegmentReader.java`, `CommitLog.java`, `CommitLogReader.java`, `CommitLogFormat` (wherever it lives) | Thread a `ChecksumType` through the existing `desc.version`-conditional path (`calculateClaimedChecksum(reader, desc.version)`) so a future commit-log format version can select CRC32C; no new version added yet, no behavior change. |
| `hints/HintsWriter.java` + Encrypted/Compressed variants, `HintsBuffer.java`, `HintsDescriptor.java`, `io/util/ChecksummedDataInput.java` + variants | Parameterize `FBUtilities.updateChecksum*` call sites on `ChecksumType`/`Checksum` interface instead of concrete `CRC32`; default preserved. |
| `utils/FBUtilities.java` | Generalize the two `CRC32`-specific `updateChecksum(CRC32, ...)` overloads to the `java.util.zip.Checksum` interface (already true for the Short/Int/Long helpers) so callers can pass a CRC32C `Checksum` instance without a new overload family. |

### 3.2 Tests proving byte-identical output

- Unit tests per touched class asserting the produced frame bytes / commit
  log segment bytes / SSTable digest bytes / hint file bytes are
  byte-for-byte identical before and after the refactor, for a fixed input
  and fixed default `ChecksumType.CRC32` — i.e. golden-file comparison
  tests (many of these files already have such fixtures, e.g.
  `CommitLogDescriptorTest`, `CompressedSequentialWriterTest`,
  `HintsDescriptorTest`; extend rather than duplicate them).
- Full existing unit test suite for `net`, `db.commitlog`, `io.compress`,
  `io.util`, `hints` packages must pass unmodified (no new assertions
  needed — regression is "did the existing suite still pass," which is the
  correctness bar for a behavior-preserving refactor).
- A round-trip dtest (single version) confirming a node written with
  pre-refactor code can be read by post-refactor code and vice versa
  (build both, cross-read) for commit log and SSTable — since this phase
  touches on-disk format code even though it introduces no new format.

### 3.3 Risk assessment

- **Risk: subtle behavior change disguised as refactor.** The `Crc.java`
  priming-byte behavior (lines 38, 58-63) is easy to accidentally drop or
  reorder when generalizing to a parameterized helper. Mitigate with the
  byte-identical golden tests above before touching call sites.
- **Risk: performance regression from added indirection** (virtual dispatch
  through `ChecksumType` instead of a direct `new CRC32()` call) on the
  hottest paths (commit log per-mutation checksum, internode frame
  encode). Mitigate: re-run the existing `ChecksumBench.java` before/after
  Phase 1 to confirm no measurable regression from the abstraction itself
  (distinct from the Phase 0 CRC32-vs-CRC32C comparison).
- **No checksum-mismatch-as-corruption risk in this phase** — behavior is
  unchanged, so there is nothing new to misinterpret as corruption.

### 3.4 Non-goals

- No new `ChecksumType.CRC32C` caller anywhere. No new `MessagingService`
  version. No new SSTable format version. No CRC24 changes. Adding
  `CRC32C` to the enum itself is included only so Phase 2/3 have a stable
  name to reference — it must remain dead code (unreferenced) at the end
  of Phase 1, verified by a "no callers" grep as part of code review.

---

## 4. Phase 2 — Internode CRC32C, negotiated per connection in the handshake

> **Status: implemented** (branch `CASSANDRA-16360-crc32c-plan`), except the
> mixed-version dtests and the frame-path JMH re-run in §4.4. Those must land
> before `internode_checksum_type: crc32c` is documented as recommended (§7).

**Goal:** let two nodes that both support it use CRC32C for internode frame
payload checksums, keep CRC32 with every peer that doesn't, and never send a
peer something it cannot decode.

### 4.1 Design

The connecting node asks; the accepting node answers; only then is anything
framed.

- **Request.** `HandshakeProtocol.Initiate` sets bit 5 of its flags when this
  node has `internode_checksum_type: crc32c`, the connection is a messaging
  connection, and its framing is `CRC` or `LZ4` (the two framings with a
  payload checksum). The framing id is unchanged.
- **Older peers ignore the request.** `Initiate.maybeDecode`
  (`HandshakeProtocol.java:155-196`) reads fixed bit ranges — connection type
  (bits 0-1), framing (bits 2 and 4), streaming (bit 3), versions (bits 8-31)
  — and never checks bits 5-7; the same holds on `cassandra-4.0`, `-4.1`,
  `-5.0` and `-6.0`. So a release that predates this treats the message
  exactly as before and answers with an ordinary `Accept`.
- **Acknowledgement.** A node running this code that receives the request
  sets bit 30 of the first `Accept` int (`maxMessagingVersion`) and installs a
  CRC32C frame decoder. It decodes CRC32C whatever its own
  `internode_checksum_type`; only the initiator's setting decides. The bit is
  only ever sent to an initiator that set the request bit, so older
  initiators never see it, and an `Accept` without it is byte-identical to
  earlier releases (`HandshakeProtocolCrc32cTest`).
- **Switch.** The initiator installs its frame encoder only after decoding
  `Accept` (`OutboundConnectionInitiator.java:360-413`), so it uses CRC32C
  exactly when it asked and the peer acknowledged, and CRC32 otherwise.
  Nothing is framed before both sides agree.
- **Both framings.** `FrameEncoderCrc`/`FrameDecoderCrc` and
  `FrameEncoderLZ4`/`FrameDecoderLZ4` each have a CRC32C variant; the CRC32
  instances (`FrameEncoderCrc.instance`, `FrameEncoderLZ4.fastInstance`),
  which the native protocol also uses (§1(b)), are byte-for-byte unchanged.
  With the default `internode_compression: dc`, cross-DC (LZ4) connections
  get CRC32C as well as intra-DC (CRC) ones.
- **Unprimed.** CRC32C is computed without `Crc.java`'s magic-prefix priming,
  through a thread-local `CRC32C` (`Crc.computeCrc32c`), so the trailer for a
  payload equals the published CRC-32C check values (`FramingTest` pins
  `"123456789"` → `0xE3069283`). Priming adds nothing here: CRC-32C starts
  from an all-ones register, so zero bytes do not checksum to zero, and the
  frame header's CRC24 (`CRC24_INIT = 0x875060`) rejects zeroed headers
  before the payload checksum is read.
- **No gate beyond the opt-in.** No `storage_compatibility_mode` condition,
  no cluster-metadata version check, no new `MessagingService.Version`.
  Every connection works out its own answer, so the setting is safe at any
  point of a rolling upgrade or downgrade, and a newly upgraded peer gets
  CRC32C on the next reconnect without a restart.
- **Framing id 3 stays unassigned.** `Framing.forId` throws
  `UnknownFramingException` for it, as for any unknown id.

### 4.2 Designs considered and rejected

1. **Negotiate the messaging version, lowest common wins** (the original
   draft). Framing is fixed in the first message, before any version is
   agreed, and 6.0 and trunk share `VERSION_60`, so a messaging version can't
   tell a peer that supports CRC32C from one that doesn't.
2. **A new framing id, `CRC32C(3)`, gated on `storage_compatibility_mode:
   NONE`.** Implemented first. Wrong in two ways: `NONE` is the shipped
   default and the mode only tracks majors, so it said nothing about whether
   a peer could decode id 3; and an old peer rejects an unknown framing id
   (its `forId` throws) where it ignores an unknown flag bit. It also used
   the last value of the 2-bit framing field and left LZ4 connections on
   CRC32.
3. **The same framing id, gated on each peer's release version from cluster
   metadata.** Safe while metadata is accurate, but it needed an extra
   rolling restart after an upgrade (a connection pool fixes its framing when
   created), had a failure window after a downgrade until metadata caught
   up, and kept the costs of design 2.

The request/acknowledge design removes all of these. Its one requirement is
that bits 5-7 of the `Initiate` flags stay unvalidated in every supported
release; that holds for 4.0 through 6.0 and trunk (§4.1).

### 4.3 Files changed

- `net/HandshakeProtocol.java` — the request bit in `Initiate`, the
  acknowledgement bit in `Accept`.
- `net/OutboundConnectionInitiator.java` — sends the request, picks the
  encoder from the acknowledgement, and reports the result in
  `MessagingSuccess.payloadChecksum`.
- `net/InboundConnectionInitiator.java` — acknowledges and picks the decoder;
  logs the negotiated payload checksum with each new connection.
- `net/FrameEncoderCrc.java`, `net/FrameDecoderCrc.java`,
  `net/FrameEncoderLZ4.java`, `net/FrameDecoderLZ4.java`, `utils/Crc.java` —
  CRC32C payload checksums.
- `net/OutboundConnection.java`, `net/OutboundConnections.java`,
  `net/InboundMessageHandlers.java`, `metrics/InternodeOutboundMetrics.java`,
  `metrics/InternodeInboundMetrics.java` — a per-peer `PayloadChecksum` gauge
  in each direction (`CRC32`, `CRC32C`, or null when not connected).
- `net/OutboundConnectionSettings.java` — `UnknownFramingException`.
- `config/Config.java`, `config/DatabaseDescriptor.java`, `conf/cassandra.yaml`
  — `internode_checksum_type` (`crc32`/`crc32c`, default `crc32`).
- `CHANGES.txt`, `NEWS.txt`.

### 4.4 Test plan

Done (unit and in-JVM, single version):

- `HandshakeProtocolCrc32cTest`: the request is exactly bit 5 for every
  framing; the acknowledgement is exactly bit 30; both round-trip; an
  `Accept` without the acknowledgement is byte-identical to earlier releases,
  and one built byte by byte as earlier releases wrote it decodes as "no
  CRC32C".
- `ConnectionTest#testNegotiatedPayloadChecksum`: with `crc32` and with
  `crc32c`, over CRC and LZ4 framing, with and without TLS, small and large
  messages, messages are delivered and both ends report the expected
  checksum.
- `FramingTest`: CRC32C round trips and bit-flip detection for CRC and LZ4
  framing; known-answer trailers for CRC32C (unprimed) and CRC32 (primed),
  for both framings; framing id 3 is rejected.
- `InboundMessageHandlersPayloadChecksumTest`, `InternodeOutboundMetricsTest`:
  the gauges.
- `InboundConnectionInitiatorTest`: the `UnknownFramingException` log line.

Still needed:

- A JMH run of the full frame encode/decode path (not just the checksum
  call), on the hardware in §2.2, to check the frame-level gain matches the
  raw checksum gain.
- Mixed-version in-JVM dtests (none written yet):
  1. Rolling 6.0→7.0 upgrade with `internode_checksum_type: crc32c` on the
     upgraded nodes. Assert: no handshake failures; upgraded↔old
     connections report `CRC32` on both gauges; upgraded↔upgraded
     connections report `CRC32C` once either side reconnects.
  2. Downgrade one node to 6.0: its peers' next connections to it report
     `CRC32`, with no failed handshakes.
  3. Mixed settings among upgraded nodes: a connection uses CRC32C exactly
     when its initiator has `crc32c`, so A→B and B→A can differ.

### 4.5 Rollback

- **Config:** set `internode_checksum_type: crc32` and restart the node. Its
  new outbound connections use CRC32; peers that still request CRC32C keep
  getting it, because this node still decodes it.
- **Binary downgrade:** nothing to do first. The downgraded node ignores
  requests, and its peers fall back to CRC32 on their next connection.
- Nothing is written to disk in a CRC32C-dependent format in this phase.

### 4.6 Failure modes

1. **Disagreement about the checksum.** Each side decides from the same two
   bits, and the encoder is chosen after the reply, so they cannot disagree
   unless a peer sets the acknowledgement without installing a CRC32C
   decoder. That would show up as every frame failing its checksum and the
   connection resetting, on the first message; `ConnectionTest` and the
   per-peer gauges are the guard against it.
2. **Real corruption under CRC32C.** Handled by the existing
   `CorruptFrame`/connection-reset path, the same as CRC32.
3. **Unknown framing id.** Only possible from a future release that adds a
   framing. `forId` throws `UnknownFramingException`, which
   `InboundConnectionInitiator#exceptionCaught` logs as a version mismatch,
   distinctly from a corruption error.

### 4.7 Availability and default

- **Opt-in** from the release that ships this phase, safe to set at any
  point of a rolling upgrade or downgrade.
- **Default:** keep `crc32`. On the evidence in §2.2, CRC32C helps on Ivy
  Bridge, probably still about 2× on Haswell/Broadwell (inferred), and not at
  all on current x86 and ARM. Revisit after the §2.2 runs that are still
  open and the frame-path measurement in §4.4. Flipping the default is a
  config change; negotiation keeps it safe in a mixed cluster.

---

## 5. Phase 3 — Native protocol v6: CRC32C-only framing, CRC24 removed

**Goal:** define native protocol v6 framing using CRC32C only (drop CRC24
entirely, since v6 is a clean break, not required to interoperate
byte-for-byte with v5 framing), staged behind the existing beta-protocol
mechanism. Independent of Phase 2 — a driver or client could adopt v6
without any internode CRC32C rollout, and vice versa.

### 5.1 Design

- `ProtocolVersion.java`: add `V7` (next ordinal after current `V6`, since
  `V6` already exists as the beta slot per `ProtocolVersion.java:51`) —
  **or**, if `V6` itself is still unreleased/still beta at implementation
  time, define the CRC32C framing as what `V6` beta graduates into,
  avoiding burning a version number on a beta that never shipped. Confirm
  `V6`'s ship status against `NEWS.txt`/JIRA before deciding which ordinal
  gets the new framing — this is a decision to make at implementation
  time, not baked into this plan.
  `BETA = Optional.of(V_new)`, following the exact existing pattern at
  `ProtocolVersion.java:87-88`.
- The v5 framing already exists: `PipelineConfigurator.frameDecoder`/
  `frameEncoder` (lines 387-402) pick `net.FrameDecoderCrc`/`FrameEncoderCrc`
  or the LZ4 variants by compression alone, for every protocol version that
  uses framing (§1(b)). The new version needs those methods to take the
  negotiated `ProtocolVersion` too, and to return a CRC32C codec for it.
  Phase 2's `FrameEncoderCrc.getInstance(ChecksumType.CRC32C)` and
  `FrameDecoderCrc.create(allocator, ChecksumType.CRC32C)` are a starting
  point, but they keep the CRC24 header, so dropping CRC24 needs a codec with
  a different header layout, not just a different trailer checksum.
- CRC24 is not migrated, not parameterized, not touched — it is simply
  absent from the new v6/v7 framing header. Document the header format
  change explicitly in the spec (§5.2).
- Gate activation the same way v6-beta is gated today: `isBeta()` +
  `Header.Flag.USE_BETA` opt-in (`Envelope.java:441`), so no client is
  affected unless it explicitly asks for the new protocol version.

### 5.2 Spec changes

New `doc/native_protocol_v6.spec` (or v7, per the ordinal decision above),
derived from `doc/native_protocol_v5.spec`, with:
- Section on frame framing (mirroring v5 spec lines ~84-173) rewritten:
  drop the CRC24 header field and keep a single CRC32C trailer covering the
  whole frame (header + payload), or keep the v5 split (a header checksum,
  so the length can be trusted before the payload arrives, plus a payload
  trailer) with CRC32C in both places. Today's `FrameEncoderCrc` uses the
  split: a CRC24 header check and a CRC32 payload trailer. Dropping the
  header checksum means a corrupted length field is only detected after
  reading up to 128KiB, which is the trade-off to settle with driver
  maintainers before finalizing the spec text.
- Explicit note in the "Changes from v5" preamble (the v5 spec has an
  equivalent "Changes from v4" section per existing convention) stating
  CRC24 is removed and why (CRC32C is JIT-intrinsified and a single
  32-bit checksum suffices; CRC24's header-only scope was a v5-era
  trade-off no longer needed).

### 5.3 Files to change

- `transport/ProtocolVersion.java` — new version constant + beta flag.
- `transport/PipelineConfigurator.java` and `transport/SimpleClient.java` —
  select the framing codec by protocol version as well as compression.
- A new frame codec without the CRC24 header (in `net`, next to
  `FrameEncoderCrc`, so both packages can share it), unless the spec keeps
  the split header (§5.2).
- `doc/native_protocol_v6.spec` (new file).
- Driver-facing: none in this repo, but flagged as an external dependency
  (§5.4).

### 5.4 Driver coordination (external dependency)

Native protocol v6/v7 is unusable until at least the major driver
ecosystems (Java, Python, DataStax/community drivers) implement the new
framing and opt in via `USE_BETA`. This is outside this repo's control and
must be tracked as a separate cross-project coordination effort — Phase 3
code landing in Cassandra does not imply the feature is usable by real
clients until driver support lands. See §6 for the entry criterion tying
protocol v6/v7 graduation out of beta to driver adoption count.

### 5.5 Test plan

- Unit: frame encode/decode round-trip for the new version, byte-for-byte
  against a hand-computed CRC32C fixture (known-answer test vectors from
  the CRC32C Castagnoli polynomial, cross-checked against
  `java.util.zip.CRC32C` output for a fixed input).
- Protocol-level: `SimpleClient`-based test connecting with
  `USE_BETA` + new version, confirming a non-opted-in client is rejected
  or downgraded per existing `Envelope.java:441` logic (extend the
  existing beta-rejection test, don't write a new mechanism).
- cassandra-stress comparative run (§2.4) v5 vs v6/v7.
- Negative test: truncate/flip a byte in a v6/v7 frame and confirm a clean
  protocol error (not a server crash, not silent acceptance). Extend the
  v5 framing coverage in `transport/CQLConnectionTest.java`, which already
  exercises `FrameEncoderCrc`/`FrameDecoderCrc`, rather than starting a new
  harness.

### 5.6 Rollback story

Since the feature is beta-gated behind explicit client opt-in, rollback is
trivial: no client is affected by default. Removing the feature before GA
is a matter of not graduating it out of beta; already-shipped beta code
can remain dormant indefinitely without a migration concern, since no
persistent state depends on it.

### 5.7 Non-goals

- Not touching internode framing (Phase 2 is fully independent).
- Not implementing incremental/streaming checksum validation mid-frame
  (matches current v5 behavior of whole-payload validation).
- Not designing a v7 feature beyond the framing change unless bundled with
  other independently-justified v7 work — this phase's scope is the
  checksum change only.

### 5.8 Highest-risk spot

A driver that partially implements v6/v7 (e.g. advertises support but
still computes CRC24) would produce frames the server rejects as checksum
failures indistinguishable from real corruption. Mitigate by making the
protocol negotiation itself (`STARTUP`/`SUPPORTED` message exchange)
robust — a driver must not be able to claim v6/v7 support without the
server having a way to sanity-check the very first frame (e.g. the
`STARTUP` frame itself, which is always well-known content) and produce a
specific "framing mismatch, not corruption" error if the first-frame CRC32C
check fails immediately after a version handshake — same principle as
§4.6, applied to the client-facing protocol.

---

## 6. Phase 4 (optional, separate ticket) — On-disk formats

Not implemented under CASSANDRA-16360's scope per this plan; tracked as a
future, separately-JIRA'd effort, summarized here for completeness since
the ticket's title covers "CRC32 is inefficient" broadly and on-disk is the
largest remaining hardcoded surface (commit log, compressed SSTable chunk
checksums, hints).

On-disk CRC32 is at least as hot per byte as internode framing, so on CPUs
where CRC32C wins, the win here is likely larger:
- every compressed chunk read verifies a CRC32 (`io/util/CompressedChunkReader.java:160,195`),
  and `crc_check_chance` defaults to 1.0 (`schema/TableParams.java:445`);
- every mutation is CRC32'd into the commit log (`db/commitlog/CommitLog.java:315-322`);
- every flush and compaction writes chunk checksums
  (`io/compress/CompressedSequentialWriter.java:55`).

Measure this before deciding Phase 4 isn't worth it: the share of read-path
and write-path CPU spent in these calls on the §2.2 hardware.

- Bump SSTable format version (new two-letter version string in
  `BigFormat.java`, e.g. next after "pa"/"pb" — confirm current letter at
  implementation time) with `Version.hasCrc32c()` gate following the exact
  `compareTo("<letter>") >= 0` idiom already used for `hasMetadataChecksum`
  etc.
- Read path selects checksum algorithm by the version recorded in the
  SSTable descriptor/commit-log-descriptor at read time — never by global
  config — so old files remain readable forever, matching the
  **Adler32 → CRC32 precedent** from ~3.0 (`ChecksumType.ADLER32` still
  exists in the enum today specifically so old files written with Adler32
  checksums remain readable, even though no writer has emitted Adler32 in
  years — confirmed via inventory: zero live `ChecksumType.ADLER32`
  callers found, meaning the *writer* migration completed but the *enum
  value* was correctly kept forever for the reader side).
- Migration to CRC32C-checksummed files only happens via `upgradesstables`
  or compaction rewriting data at the new format version — never an
  in-place checksum swap.
- Commit log: new segments get a bumped `CommitLogDescriptor.version` that
  selects CRC32C; old segments remain replayable via the existing
  version-conditional `CommitLogFormat` path identified in §1(c).
- Hints: same pattern via `HintsDescriptor` version, currently unused for
  this purpose per the inventory — would need to start being read at hint
  file open time.
- This phase is the only one where a bug could manifest as **silent data
  loss** rather than a clean error (a corrupted file misread as a
  different checksum algorithm may, in rare cases, pass a CRC check on
  garbage data rather than failing it) — treat this as requiring the
  highest test bar of all four phases if/when it is scoped: exhaustive
  version-tag fuzzing, not just happy-path round-trips.

---

## 7. Per-branch version enablement decision table

Verified directly against actual branch contents (`upstream/cassandra-4.0`,
`upstream/cassandra-4.1`, `upstream/cassandra-5.0`, `upstream/cassandra-6.0`,
local `trunk`) rather than assumed:

- `cassandra-4.0` / `cassandra-4.1` `build.xml`: no `java.supported`
  property (introduced only in 5.0+); instead both use
  `java.version.8`/`java.version.11` ant conditions accepting **either**
  JDK 8 or JDK 11 as a valid build/run target — JDK 8 is still a
  first-class supported target on both branches.
- `cassandra-5.0` `build.xml:47-48`: `java.default=11`, `java.supported=11,17`.
- `cassandra-6.0` `build.xml:47-48`: `java.default=11`, `java.supported=11,17,21`.
- `cassandra-6.0` `CHANGES.txt` top entry: `6.0-alpha3` (upstream fetched
  2026-10-11) — **6.0 is pre-GA, currently in alpha**, not yet released.
- `cassandra-6.0`'s `net/MessagingService.java` has the **identical**
  `Version` enum as trunk today (top ordinal `VERSION_60(14)`,
  `maximum_version = VERSION_60`) — trunk has not diverged from the 6.0
  branch on this file yet. Trunk's `CHANGES.txt` banner of "7.0" does not
  mean an enum bump was missed; it means trunk is presently serving as
  staging ground both for the tail of the still-alpha 6.0 cycle and the
  earliest 7.0-labeled work. **A next `MessagingService` ordinal does not
  exist anywhere in the tree yet** — re-verify immediately before Phase 2
  implementation, since 6.0 may have cut its own diverged stable branch by
  then.
- `cassandra-5.0`'s `MessagingService.java` tops out at `VERSION_50(13)`,
  `maximum_version = VERSION_50` — confirms 5.0 predates the TCM/index-hints
  work (`VERSION_60`) present on 6.0/trunk.

| Branch | Min supported JDK (verified) | CRC32C intrinsic available? | What ships | Rationale |
|---|---|---|---|---|
| 4.0 (GA, bugfix-only) | JDK 8 or 11 (`java.version.8`/`java.version.11`, no `java.supported` gate) | No on JDK 8 (`CRC32C` class added JDK 9) — pure-Java fallback would **regress** vs today's intrinsic CRC32 when running on JDK 8 | **Nothing.** No plumbing, no CRC32C. | GA branches take bugfixes only, not features (ASF release policy); JDK 8 remains a live, supported target here, so even *offering* CRC32C risks a net performance loss for JDK-8 deployments (Phase 0's PureJavaCrc32C row quantifies this). |
| 4.1 (GA, bugfix-only) | JDK 8 or 11, same as 4.0 | Same JDK-8 caveat as 4.0 | **Nothing.** | Same rationale as 4.0. |
| 5.0 (GA, bugfix-only) | JDK 11 or 17 (`java.supported=11,17`) | Yes — JDK 11 floor means the intrinsic is always available | **Nothing new**, despite intrinsic availability. `MessagingService.maximum_version = VERSION_50` — no `VERSION_60`-equivalent negotiation slot even exists on this branch to attach CRC32C to. | Feature-vs-bugfix policy for GA branches; also, introducing a new negotiated `Framing`/`Version` value via a 5.0.x patch release would let two nodes both "on 5.0.x" refuse to interoperate depending on exact patch level — an upgrade-matrix cost disproportionate to the benefit. Phase 1 plumbing (behavior-preserving, no new negotiated capability) could theoretically be backported without this risk, but is not recommended and not part of this plan. |
| 6.0 (**currently 6.0-alpha3, pre-GA**) | JDK 11, 17, or 21 (`java.supported=11,17,21`) | Yes | **Candidate for Phase 2 (internode CRC32C) and Phase 3 (protocol v6/v7 beta)**, if — and only if — 6.0 is still pre-GA (alpha/beta) when this work is implemented. Re-confirm 6.0's release stage at that time; once 6.0 ships GA it drops to the bugfix-only row above and misses this window. | 6.0 being alpha, not GA, is the state in which the project introduces new wire capabilities. Phase 2 needs no new `MessagingService.Version` and no version constant: it negotiates per connection, so it can land in any pre-GA release. |
| trunk (next major after 6.0; currently labeled 7.0 per `CHANGES.txt`; `MessagingService.Version` enum not yet bumped past 6.0's `VERSION_60` as of this writing, since trunk and 6.0 have not diverged on this file) | JDK 11, 17, or 21 (`build.xml:47-48`, identical to 6.0) | Yes | Same as 6.0 — **Phase 1 + Phase 2 + Phase 3** — trunk is always a valid target for new feature work regardless of what 6.0 does. The implementation targets trunk. | Trunk is the perpetual feature-development line; if 6.0 goes GA before this work lands, trunk (as the emerging 7.0) becomes the *only* branch this targets, per the 6.0 row's caveat above. |
| Next major after whichever branch (6.0 or 7.0) first ships this | Same JDK floor as its predecessor, re-verify | Yes | `internode_checksum_type: crc32c` could become the shipped **default** (see entry criteria below); protocol v6/v7 graduates out of beta if entry criteria are met. | Negotiation makes a default flip safe in a mixed cluster at any time; the constraint is the performance evidence, not compatibility. |

### Entry/exit criteria

- **CRC32C internode payload checksums available for opt-in**: from the
  release that ships Phase 2 (§4). Each connection negotiates, so there is
  no version constant to keep in step and no `MessagingService` ordinal
  question.
- **`internode_checksum_type: crc32c` becomes the shipped default**: only
  after (a) benchmarks show a gain on hardware the project's users still run,
  which §2.2 does not yet show beyond Ivy Bridge (see §4.7), and (b) the
  mixed-version dtests in §4.4 are green in CI. Recommend this happens no
  earlier than the first alpha of the release after the one that carries
  Phase 2, giving a full alpha/beta soak period.
- **Protocol v6/v7 leaves the beta flag**: requires (a) Phase 0 gate
  passed, (b) protocol-level tests in §5.5 green, (c) **at least 2 actively
  maintained driver implementations** (recommend: the Java driver and one
  other, e.g. Python or Gocql) demonstrating working `USE_BETA` interop in
  CI or a documented interop test report, since a beta protocol with zero
  driver support graduating to non-beta would strand the ecosystem. This
  numeric bar (N=2) is a recommendation for the release manager to ratify,
  not a hard technical constraint enforced by code.

### Deprecation path for CRC32-based framing

- CRC32 internode framing (`Framing.CRC`) remains supported **through the
  same deprecation horizon as `MessagingService.VERSION_40`/`VERSION_50`
  today** — i.e. indefinitely, until a future major release's minimum
  supported-peer version rises above the last version that only knows
  CRC32. Do not set a removal date now; removal is gated on the
  minimum-supported-version policy the project already uses for messaging
  versions generally (see `MessagingService.java` deprecated markers on
  `VERSION_30`/`VERSION_3014` as the precedent for how long "deprecated but
  still functional" lasts in practice — multiple major versions).
- CRC32-based *on-disk* formats (Phase 4, out of scope here) follow the
  Adler32 precedent: the checksum-type enum value is **never removed**,
  even after no writer emits it, because old files must remain readable
  forever. This is the strongest possible answer to "when is CRC32
  removed from disk-format support": never, as a matter of policy, only
  superseded as the default writer choice.
- CRC24 has no deprecation path to design — it is simply absent from the
  new protocol version; existing v5 clients keep using CRC24 for the
  lifetime of protocol v5's support, following whatever protocol-version
  support window the project already commits to for v3/v4/v5 today.

---

## 8. ADR: CRC32 → CRC32C migration approach

**Context.** CASSANDRA-16360 observes that `java.util.zip.CRC32`, while
JIT-intrinsified, computes IEEE 802.3 CRC-32, which x86 has no dedicated
instruction for: SSE4.2's `crc32` instruction implements only the
**Castagnoli (CRC32C)** polynomial, so the JDK's x86 CRC32 intrinsic folds
with `pclmulqdq` instead. ARMv8's CRC extension has instructions for both
polynomials. `java.util.zip.CRC32C`, added in JDK 9, uses the dedicated
instruction directly. Phase 0 (§2.2) measured the effect: a 6.6–7.9× gain on
Ivy Bridge, whose `pclmulqdq` is slow, and parity within ±1% on a recent
AMD EPYC (§2.2 RUN 3) and Apple M1. Cassandra currently hardcodes CRC32
(and, in native protocol v5 framing, also CRC24) across internode framing,
the native client protocol spec, and on-disk formats (commit log, SSTable
chunks, hints), all without a `ChecksumType`-style abstraction consistently
applied and, critically, without any of these call sites keying algorithm
choice off a version negotiated with the reader.

**Decision.** Migrate incrementally, per the four-phase plan above:
plumbing first (Phase 1, behavior-preserving), then two independent
opt-in rollouts: internode payload checksums requested by operator opt-in and
negotiated per connection in the handshake (Phase 2), and native protocol
beta-flag staging (Phase 3) — with on-disk formats deliberately deferred to a
separate ticket (Phase 4) because they carry the highest risk of the
migration (a mis-negotiated on-disk checksum reads as silent data
corruption or data loss, not a clean protocol error, unlike the wire-level
phases where a mismatch is always caught as a connection/handshake
failure). No branch other than trunk receives new capability, per the
project's GA-branch feature-vs-bugfix policy, and no branch below the
JDK-9 floor is offered CRC32C at all, since the pure-Java fallback there
would be a net regression versus the already-intrinsic CRC32 baseline —
this is quantified, not assumed, by Phase 0's benchmark gate before any
Phase 2/3 code lands.

**Consequences.**
- Positive: on CPUs with slow `pclmulqdq` (Sandy Bridge, Ivy Bridge), CPU
  spent on internode checksums drops sharply, and on client traffic once
  drivers catch up. On current hardware the effect is neutral (§2.2). Every
  new behavior is opt-in, and internode CRC32C is only used on connections
  where both peers agreed to it.
  On-disk formats keep their current CRC32 cost until a separately-scoped
  Phase 4 lands. That is accepted because the risk there (silent corruption
  on a format bug) is categorically worse, not because the CPU win is
  smaller: on-disk CRC32 is at least as hot per byte (§6), and that has not
  been measured.
- Negative: two checksum algorithms (three during the internode
  transition window, counting CRC24's continued presence in v5) must be
  maintained in the codebase indefinitely — this is accepted as the
  necessary cost of never breaking backward compatibility, matching the
  project's existing tolerance for maintaining `ADLER32` in `ChecksumType`
  years after its last writer.
- Negative: protocol v6/v7 CRC32C support is only as useful as driver
  adoption allows: this plan explicitly does not control that adoption
  and flags it as an external dependency with a numeric (but
  release-manager-ratified, not hard-coded) entry bar for graduating out
  of beta.
- Follow-up flagged, not actioned here: `ChecksumType.ADLER32` appears to
  have zero live callers; worth a separate, low-risk cleanup ticket to
  confirm it's truly only needed for legacy-file readability and document
  that explicitly in the enum's javadoc, but out of scope for
  CASSANDRA-16360 itself.
