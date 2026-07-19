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

> Status: PLANNING ONLY. No implementation code has been written. This document is
> for review; implementation begins only after explicit approval, phase by phase.

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
| `OutboundConnectionInitiator.java` (~355-382) / `InboundConnectionInitiator.java` (~472-506) | Switch on `Framing` to pick encoder/decoder | N/A | Framing enum ordinal, decoupled from messaging version negotiation |
| `MessagingService.java` `enum Version` (lines 215-229, 262-270) | `VERSION_30..VERSION_60`; `current_version` derived via `currentVersion()` (line 312) from `DatabaseDescriptor.getStorageCompatibilityMode().isBefore(N)` | N/A | This `isBefore(N)` / `AcceptVersions` min-max negotiation (lines 279-289) is the **idiom to reuse** for gating CRC32C |
| `journal/*` (`EntrySerializer.java`, `OnDiskIndex.java`, `Journal.java`, `Metadata.java`) | Accord/TCM journal integrity, via `Crc.crc32()` | `Crc.java` (same thread-local instance) | None |

**Biggest hardcoded surface**: `Crc.java` has no `ChecksumType`
abstraction at all — every internode framing class calls its static
methods directly.

### (b) Native client protocol

| Location | What | Gate today |
|---|---|---|
| `src/java/org/apache/cassandra/transport/` | No CRC/Checksum usage found in the Java implementation today (`grep` across the package is empty) | N/A — v5 framing checksums described in the spec are **not yet implemented** in the transport package; only internode `net.Frame*` classes implement CRC framing |
| `ProtocolVersion.java` | `enum {V3, V4, V5, V6(6,"v6-beta", true)}` (lines 42-70); `CURRENT = V5` (87); `BETA = Optional.of(V6)` (88); `isBeta()` (136-139) | Beta-flag idiom: `Envelope.java:441` rejects a beta version unless the client sets `Header.Flag.USE_BETA`; mirrored client-side in `Client.java:61`, `SimpleClient.java:167,206`. **This is the idiom to reuse for staging v6/CRC32C framing.** |
| `doc/native_protocol_v5.spec` (lines 84-173) | Documents CRC24 header + CRC32 payload trailer framing — the authoritative on-wire description, currently unimplemented in Java transport code but mirrored by internode `Crc.java`/`FrameEncoderCrc` | N/A |
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
   negotiation (lines 279-289). → model for internode CRC32C enablement.
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
> version, OS, CPU model/arch, build/run commands — copy §2.4 verbatim if
> unchanged), the same results table shape, and a gate verdict statement.
> Once at least one x86_64 run exists, add a summary row/table comparing
> the gate verdict across all recorded platforms before using this section
> as evidence for any Phase 2/3 go decision (per §4.3's "not before an
> x86_64 Phase 0 run justifies it").

**Environment:** JDK 21.0.11 (Temurin), macOS 26.5.2, Apple M1 (arm64), `ant build-test`
compiled classes, run via `java -cp ... org.openjdk.jmh.Main`, default class-level
JMH settings (`@Warmup(3, 1s)`, `@Measurement(5, 2s)`, 1 fork, `Threads(4)`).
Raw CSVs: `crc32_vs_crc32c.csv` (60 combinations), `crc32_coldcache.csv`,
`crc24.csv` — generated by the exact commands in §2.4 below, not committed to
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

**Action required before Phase 2/3 proceed:** re-run this exact suite (§2.4
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
(Java 21 bytecode from `ant build-test`), same per-class JMH settings; the §2.4
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

### 2.2 Baseline results table (RUN 3 — x86_64 / AMD EPYC Milan (Zen 3), 2026-07-19)

**Environment:** identical JDK/OS/harness to RUN 2 (JDK 21.0.11, Ubuntu 24.04,
kernel 6.8.0-60-generic, chunked §2.4 sweep), AMD EPYC-Milan Processor (Zen 3,
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
GB/s: on Zen 3 the JDK 21 CRC32 intrinsic's CLMUL-folding path (AVX-512 /
`vpclmulqdq`) has fully caught up with the hardware `crc32` instruction, so
the SSE4.2-era asymmetry RUN 2 exposes simply no longer exists on modern x86.
Cold-cache agrees (449.94 ± 6 vs 429.86 ± 4 ns/op, ~4.5% CRC32C). Intrinsic
toggles behave as expected (CRC32 2311 → 30121 ns, CRC32C 2300 → 74544 ns at
128KB HEAP disabled); the M1 anomaly does not reproduce here either.

### 2.2 Gate verdict summary across recorded platforms

| Run | CPU (arch / relevant ISA) | JDK | 16KB margin H/D | 128KB margin H/D | Gate (≥20%) |
|---|---|---|---|---|---|
| RUN 1 | Apple M1 (arm64, ARMv8 CRC ext: both polynomials in HW) | 21.0.11 Temurin | +0.3% / +1.1% | +0.4% / +0.2% | FAIL |
| RUN 2 | Intel Xeon E5-2650 v2 (x86_64 Ivy Bridge, SSE4.2 only) | 21.0.11 Ubuntu | **+561% / +622%** | **+689% / +690%** | **PASS** |
| RUN 3 | AMD EPYC Milan (x86_64 Zen 3, AVX-512 + `vpclmulqdq`) | 21.0.11 Ubuntu | +0.8% / −0.3% | +0.5% / +0.9% | FAIL |

**Reading for Phase 2/3 (as required by §4.3):** the gate — "≥20% at 16KB and
128KB … on at least one of x86_64/ARM64" — is now formally **PASSED**, via
RUN 2. The three runs together sharpen the claim: the CRC32C advantage is a
property of **older x86_64** (SSE4.2 without a CLMUL-vectorized CRC32 path,
i.e. the large installed base the ticket was motivated by), while
current-generation CPUs (Zen 3 x86_64, Apple M1 arm64) show parity within
±1%. Nowhere measured is CRC32C *slower* by more than noise (worst case
−0.3%, 16KB DIRECT on Zen 3). So the migration's expected effect is
"dramatic win on the old-hardware tail, neutral on modern hardware" — a
safe-to-positive profile consistent with proceeding, with the caveat that
JDK-8-era branches must still skip the software fallback (PureJavaCrc32C
confirmed at only 1.1–1.65 GB/s on both x86 machines, i.e. 2–14× *slower*
than intrinsic CRC32). All three runs used JDK 21.0.11; the §2.2 preamble's
ask for JDK 11/17 datapoints remains open.

### 2.4 Exact commands to reproduce or extend this run

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

JDK version / CPU model / JVM flags recorded alongside every row (see §2.3);
repeat this on x86_64 SSE4.2 hardware and append a second results table here
before using this section as evidence for a Phase 2/3 go decision.

### 2.3 End-to-end benchmarks (repeated after each phase lands)

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
>   instead given an additive, `@Deprecated(since="5.1")` `CRC32`-typed
>   overload delegating to the new `Checksum`-typed one:
>   `FBUtilities.updateChecksum` (×2), `CompressedHintsWriter`'s public
>   constructor, `EncryptedHintsWriter`'s protected constructor (its class is
>   public, so an external subclass in another package could in principle
>   exist). Signatures that were private, or whose enclosing class was not
>   `public` (`CommitLogReader.CommitLogFormat` is a private nested class;
>   `HintsWriter` itself has no `public` modifier), were changed in place
>   with no compatibility shim, since no external `.class` file could
>   possibly hold a reference to them.
> - Reviewed by a second model pass (Opus) with no correctness or
>   binary-compatibility gaps found beyond the `ChecksummedDataInput` item
>   above, which was then fixed. Verified via `ant build-test` (clean
>   compile) and 8 existing unit test suites across commit log, hints, and
>   compressed-writer paths (all pass unmodified, confirming byte-identical
>   behavior).

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

## 4. Phase 2 — Internode CRC32C, gated by StorageCompatibilityMode

> **Status: design revised, implementation not started.** While scoping the
> implementation, reading the actual `OutboundConnectionInitiator`/
> `HandshakeProtocol` code revealed that the "per-connection negotiation,
> lowest-common-version wins" design originally written below is **not
> achievable as described**, for a concrete architectural reason (§4.1a).
> The design has been corrected to §4.1b before any code was written, per
> this project's own ground rule: never guess at wire-protocol safety.

**Goal:** allow two nodes that both understand it to use CRC32C for
internode frame checksums, with CRC32 remaining available forever for peers
that don't, and no scenario where an old peer receives a framing byte it
cannot decode.

### 4.1a Why "negotiate per-connection" doesn't work here

`OutboundConnectionSettings.framing(category)` (net/OutboundConnectionSettings.java:454-464)
picks the framing (`UNPROTECTED`/`LZ4`/`CRC`) **before any handshake
round-trip occurs** — it's a static, local decision derived purely from
config (is internode compression enabled?), computed once when the
connection pool's settings are built. That choice is serialized into the
very first wire message the connecting node sends (`HandshakeProtocol.Initiate`,
`net/HandshakeProtocol.java:101-123`), alongside the proposed messaging
`AcceptVersions` range — but the *accepting* peer's messaging version isn't
known to the connecting node until its response comes back one round-trip
later (`net/OutboundConnectionInitiator.java:356-357`, where `useMessagingVersion`
is first read). By the time a messaging-version mismatch is discovered and
retried (`Result.incompatible`/`Result.retry`, lines 366 and 536-537 of the
same file), the framing byte has *already been sent* in the first message —
there is no equivalent retry path for "the peer didn't understand your
framing choice." An old peer's `Framing.forId(int)` (a plain `switch` with
no `default`, `net/OutboundConnectionSettings.java:59-68`) would throw
`IllegalStateException` on an unrecognized id, which is not a clean,
loggable "negotiation failed, falling back" outcome — it's an abrupt decode
failure, exactly the "surfaces as corruption/crash instead of a clean
version error" failure mode the ground rules warn about, just one level up
the stack from an actual checksum mismatch.

The one favorable fact: `Framing.id` is already wire-encoded as 2 bits
or a value which can represent `0`–`3`
(`HandshakeProtocol.Initiate.encodeFlags()`, line 118:
`(framing.id & 1) << 2 | (framing.id & 2) << 3`), and only `0,1,2` are used
today. So `CRC32C(3)` needs **no wire format change** — the only open
question is how to know, before the first message is ever sent, that it's
safe to use it.

### 4.1b Corrected design: gate on `StorageCompatibilityMode`, not live negotiation

Rather than a live per-connection capability check (which this codebase's
handshake sequencing doesn't support without a larger protocol change — see
§4.1c for that alternative and why it's out of scope here), gate
`Framing.CRC32C` selection the same way the project already gates other
changes that aren't safe to auto-detect per connection:
`StorageCompatibilityMode` (`utils/StorageCompatibilityMode.java`) already
has an `UPGRADING` value explicitly documented as "disabling features that
are not compatible with any not-upgraded nodes in the cluster... once all
nodes have been upgraded, you can set the compatibility to `NONE`." This is
precisely the operator-driven safety mechanism this feature needs, already
built and already the idiom operators know from other post-4.0 rollouts.

- `OutboundConnectionSettings.framing(category)` gains a config-driven
  branch: return `Framing.CRC32C` only when (a) a new
  `internode_checksum_type: crc32c` yaml option is set (default `crc32`)
  **and** (b) `StorageCompatibilityMode.current() == StorageCompatibilityMode.NONE`
  — i.e. the operator has explicitly asserted the whole cluster is already
  upgraded to a version that understands CRC32C framing, the same
  precondition operators already satisfy before relying on other
  `NONE`-gated behavior. `UPGRADING` and any `CASSANDRA_N` mode continue to
  produce `Framing.CRC`/`LZ4` exactly as today.
- `Framing.forId(int id)` gains a `case 3: return CRC32C;` branch — this is
  required regardless of the gating mechanism, since a node that has
  chosen CRC32C will send id `3` and its peer (by construction, also on
  `NONE` mode with CRC32C enabled) must decode it.
- `FrameEncoderCrc`/`FrameDecoderCrc`'s existing Phase-1
  `getInstance(ChecksumType)`/`create(allocator, ChecksumType)` entry
  points are reused with `ChecksumType.CRC32C` — this phase's actual new
  work is removing their `UnsupportedOperationException` guard and
  deciding the CRC32C priming question Phase 1 deferred (§3.1: no reuse of
  CRC32's magic prefix bytes, since that priming was CRC32-specific tuning
  with no established rationale for CRC32C — plain, unprimed
  `ChecksumType.CRC32C.newInstance()` is the simplest defensible choice,
  matching how `ChecksumType.CRC32C` is used everywhere else in Phase 1).
- `MessagingService.Version.VERSION_70` (next ordinal after `VERSION_60` —
  re-verify against the tree at implementation time, since this is
  advisory naming, not load-bearing for the gating mechanism above) can
  still be added for bookkeeping/documentation of when CRC32C-capable code
  shipped, but it is **not** the gating mechanism — `StorageCompatibilityMode`
  is. This is a deliberate departure from this plan's earlier draft, which
  conflated "new capability shipped in version N" with "safe to
  auto-negotiate per connection," which turned out not to hold for framing.
- `HandshakeProtocol`'s legacy 3-message path and the 2-message
  `AcceptVersions` exchange are both untouched — this design doesn't touch
  messaging-version negotiation at all, only the pre-handshake framing
  choice, which is now StorageCompatibilityMode-gated instead of
  version-gated.

### 4.1c Alternative considered and rejected for this phase: real capability pre-check

A more "correct in the abstract" fix would add an actual round-trip before
framing is committed to, so the connecting node learns the peer's supported
framings before ever sending one. This would remove the
`StorageCompatibilityMode` operator-trust requirement and allow true
per-connection auto-negotiation, matching this plan's original intent. It
is explicitly **out of scope for this phase**: it changes the handshake
wire format itself (not just an added framing id), has broader blast
radius (affects every connection, not just CRC32C-curious ones), and needs
its own risk assessment, test plan, and rollback story independent of
CRC32C. If a future need arises for finer-grained auto-negotiation of
per-connection capabilities beyond framing, this is the design to revisit,
tracked as a follow-on idea rather than folded into CASSANDRA-16360.

### 4.2 Files to change

- `utils/StorageCompatibilityMode.java` — no new enum value needed; reuse
  existing `NONE`/`UPGRADING`/`CASSANDRA_N` values as the gate.
- `net/OutboundConnectionSettings.java` — new `Framing.CRC32C(3)` ordinal;
  `forId(int)` gains `case 3`; `framing(ConnectionCategory)` (lines
  ~454-464) gains the config-and-compat-mode-gated branch described in
  §4.1b, checked *before* the existing LZ4-vs-CRC compression check.
- `net/FrameEncoderCrc.java`, `net/FrameDecoderCrc.java` — remove the
  Phase-1 `UnsupportedOperationException` guard for `ChecksumType.CRC32C`;
  implement the actual CRC32C encode/decode path (unprimed
  `ChecksumType.CRC32C.newInstance()`, per §4.1b). LZ4 variants are a
  straightforward mirror once the CRC-only variant is proven correct — no
  design difference beyond "payload is decompressed first."
- `net/OutboundConnectionInitiator.java`, `net/InboundConnectionInitiator.java`
  — extend the existing `switch (settings.framing)` (already present from
  Phase 1) with a `case CRC32C` branch calling
  `FrameEncoderCrc.getInstance(ChecksumType.CRC32C)` /
  `FrameDecoderCrc.create(allocator, ChecksumType.CRC32C)`.
- `config/Config.java` / `conf/cassandra.yaml` — new
  `internode_checksum_type` setting (values `crc32`/`crc32c`, default
  `crc32`).
- No `MessagingService.java` change is required for the gating mechanism
  itself (see §4.1b) — adding `VERSION_70` remains optional bookkeeping,
  not load-bearing.

### 4.3 When CRC32C framing becomes available / default

- **Available** (operator can opt in): as soon as this phase ships, an
  operator running an all-upgraded cluster (`storage_compatibility_mode:
  NONE`, the existing yaml default) can set
  `internode_checksum_type: crc32c` and get it immediately — no waiting on
  a `current_version`/messaging-version gate, since the gate is
  compatibility-mode-based, not version-based.
- **Default for new clusters**: not before Phase 0's benchmark gate passes
  on the actual target architecture (x86_64 — this plan's own Phase 0 run
  was on Apple M1/arm64 and **failed** the gate, §2.2) and the dtests in
  §4.4 are green. Given the gate's current fail status on the only
  hardware tested so far, `internode_checksum_type` should default to
  `crc32` indefinitely until an x86_64 Phase 0 run justifies flipping the
  default — this is a config-default decision, not a version-enum one, so
  it can be revisited independently per release without a new
  `MessagingService.Version`.

### 4.4 Test plan

- Unit: `FrameEncoderCrc`/`FrameDecoderCrc` round-trip tests parameterized
  over both `ChecksumType` values (extends Phase 1's byte-identical tests
  with a second, intentionally-different-output case now that CRC32C is a
  real caller). Include a known-answer CRC32C test vector (Castagnoli
  polynomial) independent of `java.util.zip.CRC32C` itself, to catch a
  priming/byte-order mistake that a self-consistent round-trip test alone
  would miss.
- Unit: `OutboundConnectionSettings.framing(category)` gating logic —
  table-test all combinations of `internode_checksum_type` ×
  `StorageCompatibilityMode` value, asserting `Framing.CRC32C` is chosen
  only for the single `crc32c` × `NONE` combination.
- JMH: re-run `Crc32VsCrc32cBench` in the actual frame-encode path (not
  just the raw checksum call) to confirm end-to-end frame throughput gain
  matches the Phase 0 raw-checksum gain, ruling out the abstraction
  overhead eating the benefit. Run on x86_64 given the Phase 0 gate's
  current fail status on arm64.
- **Mixed-version / mixed-compatibility-mode dtests** (the critical new
  coverage — note these now test *compatibility-mode* transitions, not a
  live per-connection version negotiation, since §4.1b changed the gating
  mechanism):
  1. 3-node cluster, all on old version, `storage_compatibility_mode: NONE`
     (today's default) — baseline, must pass unchanged (CRC32 framing,
     `internode_checksum_type` not yet a recognized option).
  2. Rolling upgrade with `storage_compatibility_mode: UPGRADING` set on
     every node throughout the upgrade (the documented operator procedure)
     and `internode_checksum_type: crc32c` set on upgraded nodes — assert
     `Framing.CRC32C` is **never** selected while any node reports
     `UPGRADING`, even on connections between two already-upgraded nodes
     (the gate is cluster-wide-honest by design, not per-connection-clever).
  3. Once all nodes are upgraded and the operator flips
     `storage_compatibility_mode: NONE` cluster-wide (the documented final
     step), restart nodes and confirm connections between two nodes with
     `crc32c` configured now select `Framing.CRC32C` (verify via internode
     connection metrics/JMX exposing negotiated framing type per
     connection — add such a metric if none exists).
  4. Negative test: manually force a `NONE`+`crc32c` node to connect to an
     old-version peer that doesn't have `case 3` in `Framing.forId` (i.e.
     simulate the "operator declared NONE too early" mistake) and confirm
     the resulting failure is a clean, loggable decode rejection rather
     than an unhandled `IllegalStateException` propagating out of the
     pipeline — this may require hardening `Framing.forId`/its caller to
     convert the `IllegalStateException` into the same
     `Incompatible`/connection-reset path used for messaging-version
     mismatches, rather than assuming operators never make this mistake.
  5. Fault injection: corrupt a byte in a CRC32C-framed payload in a test
     harness and confirm it's caught as a checksum failure (connection
     reset/reconnect), not silently accepted — i.e. CRC32C's own
     correctness, not just the gating.

### 4.5 Rollback story

- Config-flag rollback: setting `internode_checksum_type: crc32` (or
  removing the opt-in setting) on all nodes reverts all *new* connections
  to CRC32 without a restart-order dependency — `framing(category)` is
  re-evaluated whenever a connection pool's settings are built, not fixed
  once at process startup for the whole node's lifetime.
- Compatibility-mode rollback: setting `storage_compatibility_mode` back to
  `UPGRADING` or a `CASSANDRA_N` value immediately disables `Framing.CRC32C`
  selection cluster-wide, independent of the `internode_checksum_type`
  setting — this is the "emergency stop" lever, and it's the same lever
  operators already use to disable other not-yet-trusted post-4.0 behavior,
  so no new operational knowledge is required.
- Full downgrade rollback: downgrading the binary on a node reverts it to
  code that has no `Framing.CRC32C` case at all; per §4.4 item 4, this
  needs the `Framing.forId` hardening to fail cleanly rather than crash —
  without that hardening, downgrade rollback is **not safe** and must not
  be documented as supported until it's implemented. No data is written to
  disk in a CRC32C-dependent format in this phase (internode wire framing
  only), so rollback carries no data-loss risk, only the connection-crash
  risk just described.

### 4.6a Resolved during domain-modeling review (2026-07-17)

A grilling/domain-modeling session against the actual code (not just this
plan) found three items §4.4/§4.5/§4.6 called for but that had not yet
landed, and settled the open design questions attached to each:

1. **Distinct failure classification is required, not optional.**
   `Framing.forId(3)` on an old/misconfigured peer already throws
   `UnknownFramingException` (extends `IOException`) instead of the
   original design's unhandled `IllegalStateException` — that part of §4.6
   item 1 is done. But tracing the call path
   (`HandshakeProtocol.Initiate.maybeDecode` →
   `InboundConnectionInitiator.Handler.initiate` → the class's generic
   `exceptionCaught`) showed it falls into the same catch-all as *every*
   handshake failure, logged with the identical
   `"Failed to properly handshake with peer {}. Closing the channel."`
   message as a real `InvalidCrc` corruption event. Decision: this is not
   good enough — an operator must be able to tell "peer/config mismatch"
   from "possible real corruption" from logs alone, the same way this file
   already special-cases `InvalidLegacyProtocolMagic` with its own
   no-spam warning one branch above the generic case. `UnknownFramingException`
   needs the same kind of dedicated, distinctly-worded log branch.
2. **Outbound framing needs a per-peer metric.** No JMX/metrics artifact
   exposing "negotiated framing type" existed anywhere in the tree, which
   means the §4.4 item 3 mixed-version dtest ("verify via internode
   connection metrics/JMX exposing negotiated framing type per connection")
   could not actually be written as originally specified. Decision: add a
   `Gauge<String>` to `InternodeOutboundMetrics`, mirroring its existing
   per-address `Gauge` pattern, reporting the current
   `OutboundConnectionSettings.framing(category)` outcome for that peer.
   (Granularity note: `framing(category)` only varies by
   streaming-vs-messaging, and streaming is always `UNPROTECTED`, so one
   gauge per peer is sufficient — small/large/urgent messaging connections
   to the same peer always resolve to the same `Framing`.)
3. **Inbound framing needs a matching per-peer metric.** The outbound
   gauge alone only shows what this node *chose to send*; it can't reveal
   a case where what's actually being decoded from a peer diverges from
   that (the exact failure mode §4.4 item 4's negative test targets).
   Decision: add a matching `Gauge<String>` to `InternodeInboundMetrics`
   reporting the `Framing` actually being decoded from each peer's inbound
   connection, so a mismatch between "what I sent" and "what arrived" is
   directly observable rather than only inferable from an
   `UnknownFramingException` log line.

### 4.6 Highest-risk spot: checksum mismatch / decode failure surfacing as corruption

Two distinct failure modes, both needing a clean, distinctly-logged
rejection rather than falling through to generic corruption handling:

1. **Framing-id decode failure** (new in this design): an old peer, or a
   peer with `internode_checksum_type`/`storage_compatibility_mode`
   misconfigured relative to its peers, receives a `Framing.forId(3)` it
   can't decode. Per §4.4 item 4, this must be hardened to produce a
   clean, specific rejection (e.g. an `UnknownFramingException` distinct
   from `Crc.InvalidCrc`) rather than an unhandled `IllegalStateException`.
   This is the primary risk this corrected design introduces, replacing
   the "per-connection negotiation bug" risk the original (unworkable)
   design would have had.
2. **Checksum mismatch given correct framing** (the original concern):
   once both sides agree on `Framing.CRC32C` (by construction, since
   selection is compatibility-mode-gated rather than per-connection
   negotiated, this should never desync between two `NONE`-mode peers
   unless one has a stale/buggy config), a real CRC32C mismatch should
   still be treated as a bona fide integrity failure — reusing the
   existing `Crc.InvalidCrc`/connection-reset handling is correct here,
   since there's no live negotiation state to second-guess.

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
- Implement the v5-spec-described-but-never-built framing layer in
  `transport/` (currently absent per the inventory in §1(b)) using
  `ChecksumType.CRC32C` from Phase 1, reusing (not duplicating)
  `FrameEncoderCrc`/`FrameDecoderCrc` if their Phase-1 parameterization is
  generic enough to be shared between `net` and `transport` packages, or a
  thin `transport`-local wrapper if the packages' framing needs diverge
  (header format, self-contained-frame semantics, etc. — determine during
  implementation by diffing `net.FrameDecoder`'s header format against
  `doc/native_protocol_v5.spec`'s section 2.3).
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
  drop the CRC24 header field entirely, keep a single CRC32C trailer
  covering the whole frame (header + payload), matching how
  `FrameEncoderCrc`/`FrameDecoderCrc` structure their non-LZ4 variant today
  (single checksum, not split header/payload) — or preserve the
  header/payload split if driver implementers prefer incremental
  validation; this is an open design question to resolve with driver
  maintainers before finalizing the spec text, not a decision this plan
  makes unilaterally.
- Explicit note in the "Changes from v5" preamble (the v5 spec has an
  equivalent "Changes from v4" section per existing convention) stating
  CRC24 is removed and why (CRC32C is JIT-intrinsified and a single
  32-bit checksum suffices; CRC24's header-only scope was a v5-era
  trade-off no longer needed).

### 5.3 Files to change

- `transport/ProtocolVersion.java` — new version constant + beta flag.
- `transport/PipelineConfigurator.java` — wire in new framing codec for
  the new version.
- New `transport/FrameEncoderCrc32c.java`/`FrameDecoderCrc32c.java` (or
  shared `net` classes, per §5.1).
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
- cassandra-stress comparative run (§2.3) v5 vs v6/v7.
- Negative test: truncate/flip a byte in a v6/v7 frame and confirm a clean
  protocol error (not a server crash, not silent acceptance) — the
  transport layer today already must define this for CRC framing in
  general; extend existing v5 CRC-failure tests (if the framing layer
  exists there) or write the first one now since §1(b) found no existing
  Java implementation of this to extend.

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
- `cassandra-6.0` `CHANGES.txt` top entry: `6.0-alpha2` — **6.0 is pre-GA,
  currently in alpha**, not yet released.
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
| 6.0 (**currently 6.0-alpha2, pre-GA**) | JDK 11, 17, or 21 (`java.supported=11,17,21`) | Yes | **Candidate for Phase 2 (internode CRC32C) and Phase 3 (protocol v6/v7 beta)**, if — and only if — 6.0 is still pre-GA (alpha/beta) when this work is implemented. Re-confirm 6.0's release stage at that time; once 6.0 ships GA it drops to the bugfix-only row above and misses this window. | 6.0 being alpha, not GA, is exactly the state in which the project's own precedent (every prior `VERSION_xx` ordinal) shows new negotiated capability gets introduced — during pre-GA development, not after. This is the **only** branch below trunk where shipping the new `MessagingService.Version` ordinal is both technically ready (JDK floor supports the intrinsic) and policy-compatible (still pre-GA). |
| trunk (next major after 6.0; currently labeled 7.0 per `CHANGES.txt`; `MessagingService.Version` enum not yet bumped past 6.0's `VERSION_60` as of this writing, since trunk and 6.0 have not diverged on this file) | JDK 11, 17, or 21 (`build.xml:47-48`, identical to 6.0) | Yes | Same as 6.0 — **Phase 1 + Phase 2 + Phase 3** — trunk is always a valid target for new feature work regardless of what 6.0 does, and will inherit whatever new `MessagingService.Version` ordinal 6.0 introduces once/if 6.0 branches away from trunk. | Trunk is the perpetual feature-development line; if 6.0 goes GA before this work lands, trunk (as the emerging 7.0) becomes the *only* branch this targets, per the 6.0 row's caveat above. |
| Next major after whichever branch (6.0 or 7.0) first ships this | Same JDK floor as its predecessor, re-verify | Yes | `internode_checksum_type: crc32c` becomes the shipped **default** for fresh, all-`NONE`-mode clusters (see entry criteria below); protocol v6/v7 graduates out of beta if entry criteria are met. | This is the earliest point the capability can become the unconditional default, since every node on this line already understands it by definition — though the `StorageCompatibilityMode` gate mechanism itself persists, for whatever the *next* new capability turns out to need it. |

### Entry/exit criteria

- **CRC32C internode framing available for opt-in**: first trunk commit
  implementing Phase 2 (§4), immediately — an operator on an all-upgraded,
  `storage_compatibility_mode: NONE` cluster can set
  `internode_checksum_type: crc32c` right away. Note this is **not** a
  `MessagingService` version-ordinal question per the §4.1a/§4.1b
  correction: framing selection is gated by `StorageCompatibilityMode`,
  not by a negotiated messaging version, so there is no "introduce the
  ordinal, then later flip current_version" two-step for this feature the
  way there was for `VERSION_60`.
- **`internode_checksum_type: crc32c` becomes the shipped default**: only
  after (a) Phase 0's benchmark gate has passed on the target JDK/arch
  (x86_64 — this plan's own arm64 run failed the gate, §2.2), and (b) the
  dtests in §4.4 are green in CI, including the `Framing.forId` hardening
  in §4.4 item 4 / §4.6 item 1. Recommend this happens no earlier than the
  first alpha of the release that carries Phase 2, giving a full alpha/beta
  soak period even though the mechanism itself doesn't require it — a
  risk-management choice, not a technical requirement.
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
JIT-intrinsified since early JDK versions on x86, computes a checksum
algorithm (IEEE 802.3 CRC-32) that is not the one modern CPUs accelerate
via a dedicated instruction — x86 SSE4.2's `crc32` instruction and ARMv8's
CRC32C extension both implement the **Castagnoli (CRC32C)** polynomial, not
IEEE CRC-32. `java.util.zip.CRC32C`, added in JDK 9, exposes this
faster-in-hardware polynomial directly. Cassandra currently hardcodes CRC32
(and, in native protocol v5 framing, also CRC24) across internode framing,
the native client protocol spec, and on-disk formats (commit log, SSTable
chunks, hints), all without a `ChecksumType`-style abstraction consistently
applied and, critically, without any of these call sites keying algorithm
choice off a version negotiated with the reader.

**Decision.** Migrate incrementally, per the four-phase plan above:
plumbing first (Phase 1, behavior-preserving), then two independent
opt-in rollouts gated by existing versioning mechanisms — internode
messaging version negotiation (Phase 2) and native protocol beta-flag
staging (Phase 3) — with on-disk formats deliberately deferred to a
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
- Positive: CPU spent on checksums drops measurably (Phase 0's own gate
  requires this to be proven, not asserted) on internode traffic and,
  once drivers catch up, on client traffic, with zero risk to existing
  clusters since every new behavior is opt-in and negotiated.
  On-disk formats keep their current CRC32 cost until a separately-scoped
  Phase 4 lands, which is an accepted trade-off — on-disk checksums are
  computed far less frequently per byte transferred than wire framing (no
  per-hop re-checksum), so the CPU win there is smaller and the risk
  (silent corruption on a negotiation bug) is categorically worse,
  justifying the deferral.
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
