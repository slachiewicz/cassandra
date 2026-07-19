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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.utils.Crc;

/**
 * CASSANDRA-16360 Phase 0: isolates the cost of {@link Crc#crc24(long, int)}, the frame-header
 * checksum used by {@code FrameEncoderCrc}/{@code FrameEncoderLZ4} (3 and 5 byte headers
 * respectively). CRC24 is not a candidate for replacement by CRC32C — it is simply dropped in the
 * new native protocol version framing (see PLAN.md Phase 3) — so this benchmark exists only to
 * quantify what that removal saves, not to compare algorithms head-to-head.
 *
 * <p>Repeat on every JDK version and CPU architecture available, same as {@link Crc32VsCrc32cBench},
 * and record a row per (JDK, arch) combination in
 * doc/modules/cassandra/pages/architecture/crc32c-plan.md's Phase 0 results table (§2.2) -- CRC24
 * has no ISA-level hardware instruction on any architecture and its naive bit-loop implementation
 * (see {@link Crc#crc24(long, int)}'s javadoc) may still have JIT-dependent cost variance worth
 * tracking per platform.</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class Crc24Bench
{
    private static final Random random = new Random(12345678);

    // FrameEncoderCrc header is 3 bytes; FrameEncoderLZ4 header is 5 bytes (see Crc.java callers)
    private long header3b;
    private long header5b;

    @Setup
    public void setup()
    {
        header3b = random.nextLong() & 0xFFFFFFL;
        header5b = random.nextLong() & 0xFFFFFFFFFFL;
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public int benchCrc24_3byteHeader()
    {
        return Crc.crc24(header3b, 3);
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public int benchCrc24_5byteHeader()
    {
        return Crc.crc24(header5b, 5);
    }
}
