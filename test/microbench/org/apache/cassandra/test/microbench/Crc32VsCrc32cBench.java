/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.xerial.snappy.PureJavaCrc32C;

import org.apache.cassandra.utils.ChecksumType;

/**
 * CASSANDRA-16360 Phase 0 baseline: compares the current CRC32 checksum path (as used by
 * {@link ChecksumType#CRC32}, the internode framing path, and compressed-chunk checksums) against
 * java.util.zip.CRC32C (JIT-intrinsified since JDK 9) and a pure-Java CRC32C fallback, across both
 * heap and direct {@link ByteBuffer}s, at buffer sizes matching the numbers already posted on the
 * ticket (31/131/517/2041 bytes) plus frame-realistic sizes (16KB default frame payload, 128KB).
 *
 * This is a read-only benchmark: it does not change any production checksum selection. See
 * {@link Crc32ColdCacheBench} for the large-working-set variant that avoids flattering table-based
 * implementations that stay L2-resident in a tight loop, and {@link Crc24Bench} for the isolated
 * cost of the CRC24 frame-header checksum this migration drops in the new protocol version.
 *
 * <p><b>Repeat this sweep on every JDK version and CPU architecture available before drawing any
 * conclusion.</b> The one run committed so far (JDK 21, Apple M1/arm64) showed CRC32C beating CRC32
 * by well under the ticket's expected margin -- likely because ARMv8's CRC extension accelerates
 * both the IEEE CRC-32 and Castagnoli CRC32C polynomials in hardware, unlike x86 SSE4.2, which only
 * accelerates CRC32C. That means a single (JDK, arch) data point is not sufficient evidence either
 * way: record a new row in doc/modules/cassandra/pages/architecture/crc32c-plan.md's Phase 0 results
 * table (§2.2) for every (JDK version, CPU architecture) combination you run this on -- at minimum
 * JDK 11/17/21 x {@code x86_64}, plus {@code arm64} if available -- with the JDK version, CPU model,
 * and JVM flags noted alongside each row, before treating the Phase 0 gate as passed or failed for
 * any given platform.</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class Crc32VsCrc32cBench
{
    private static final Random random = new Random(12345678);

    public enum BufferKind
    {
        HEAP, DIRECT
    }

    // intentionally not on power-of-2 values for the small sizes, matching the numbers already
    // posted on CASSANDRA-16360; 16384/131072 are frame-realistic (default frame payload / 128KB)
    @Param({ "31", "131", "517", "2041", "16384", "131072" })
    private int bufferSize;

    @Param({ "HEAP", "DIRECT" })
    private BufferKind bufferKind;

    private ByteBuffer buffer;

    @Setup
    public void setup()
    {
        byte[] bytes = new byte[bufferSize];
        random.nextBytes(bytes);

        buffer = bufferKind == BufferKind.DIRECT
                 ? ByteBuffer.allocateDirect(bufferSize)
                 : ByteBuffer.allocate(bufferSize);
        buffer.put(bytes);
        buffer.flip();
    }

    private ByteBuffer buf()
    {
        return buffer.duplicate();
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public long benchCrc32()
    {
        return ChecksumType.CRC32.of(buf());
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
            "-XX:+UnlockDiagnosticVMOptions", "-XX:-UseCRC32Intrinsics",
    })
    public long benchCrc32NoIntrinsic()
    {
        return ChecksumType.CRC32.of(buf());
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public long benchCrc32c()
    {
        CRC32C crc32c = new CRC32C();
        crc32c.update(buf());
        return crc32c.getValue();
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
            "-XX:+UnlockDiagnosticVMOptions", "-XX:-UseCRC32CIntrinsics",
    })
    public long benchCrc32cNoIntrinsic()
    {
        CRC32C crc32c = new CRC32C();
        crc32c.update(buf());
        return crc32c.getValue();
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public long benchPureJavaCrc32c()
    {
        // documents the JDK8-era regression risk: no JIT intrinsic backs this path, so it is the
        // number branches without java.util.zip.CRC32C (JDK 8) would actually get if they offered
        // "CRC32C" via a software fallback instead of skipping it (see PLAN.md Phase 0 / branch table)
        PureJavaCrc32C pureJavaCrc32C = new PureJavaCrc32C();
        ByteBuffer buf = buf();
        if (buf.hasArray())
        {
            pureJavaCrc32C.update(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        }
        else
        {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            pureJavaCrc32C.update(bytes, 0, bytes.length);
        }
        return pureJavaCrc32C.getValue();
    }
}
