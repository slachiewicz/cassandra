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
import java.util.concurrent.atomic.AtomicInteger;
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

import org.apache.cassandra.utils.ChecksumType;

/**
 * CASSANDRA-16360 Phase 0: addresses the caveat raised on the ticket that a tight JMH loop over a
 * single reused buffer flatters table-based checksum implementations, because the algorithm's
 * lookup table stays L2-resident across invocations. This variant cycles through a working set of
 * buffers sized larger than a typical L2 cache (16 x 2MB = 32MB total) between invocations, so each
 * checksum call forces the table (if any) back in from a colder cache level, and reports the
 * resulting ns/op as a distinct row from {@link Crc32VsCrc32cBench}'s hot-loop numbers rather than
 * blending the two.
 *
 * <p>As with {@link Crc32VsCrc32cBench}, repeat on every JDK version and CPU architecture available
 * and record a row per (JDK, arch) combination in
 * doc/modules/cassandra/pages/architecture/crc32c-plan.md's Phase 0 results table (§2.2) -- the
 * cache-locality effect this benchmark targets is itself architecture-dependent (cache sizes and
 * associativity vary by CPU model), so a single platform's numbers don't generalize.</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class Crc32ColdCacheBench
{
    private static final Random random = new Random(12345678);

    private static final int WORKING_SET_BUFFERS = 16;
    private static final int BUFFER_SIZE = 2 * 1024 * 1024; // 2MB each; 32MB total working set, well above L2

    @Param({ "16384" }) // frame-realistic default payload size checksummed out of each working-set buffer
    private int checksumSize;

    private ByteBuffer[] workingSet;
    private final AtomicInteger cursor = new AtomicInteger();

    @Setup
    public void setup()
    {
        workingSet = new ByteBuffer[WORKING_SET_BUFFERS];
        for (int i = 0; i < WORKING_SET_BUFFERS; i++)
        {
            byte[] bytes = new byte[BUFFER_SIZE];
            random.nextBytes(bytes);
            ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
            buf.put(bytes);
            buf.flip();
            workingSet[i] = buf;
        }
    }

    private ByteBuffer nextSlice()
    {
        ByteBuffer buf = workingSet[cursor.getAndIncrement() % WORKING_SET_BUFFERS].duplicate();
        buf.limit(checksumSize);
        return buf;
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public long benchCrc32ColdCache()
    {
        return ChecksumType.CRC32.of(nextSlice());
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public long benchCrc32cColdCache()
    {
        CRC32C crc32c = new CRC32C();
        crc32c.update(nextSlice());
        return crc32c.getValue();
    }
}
