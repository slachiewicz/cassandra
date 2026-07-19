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
package org.apache.cassandra.net;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.utils.ChecksumType;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

import static org.apache.cassandra.utils.Crc.crc24;
import static org.apache.cassandra.utils.Crc.crc32;

/**
 * Please see {@link FrameDecoderCrc} for description of the framing produced by this encoder.
 */
@ChannelHandler.Sharable
public class FrameEncoderCrc extends FrameEncoder
{
    static final int HEADER_LENGTH = 6;
    private static final int TRAILER_LENGTH = 4;
    public static final int HEADER_AND_TRAILER_LENGTH = 10;

    public static final FrameEncoderCrc instance = new FrameEncoderCrc(ChecksumType.CRC32);
    // CASSANDRA-16360: a distinct singleton, not a field on `instance`, since FrameEncoderCrc is
    // @Sharable and stateless-per-connection -- see getInstance(ChecksumType) below.
    private static final FrameEncoderCrc crc32cInstance = new FrameEncoderCrc(ChecksumType.CRC32C);

    static final PayloadAllocator allocator = (isSelfContained, capacity) ->
        new Payload(isSelfContained, capacity, HEADER_LENGTH, TRAILER_LENGTH);

    private final ChecksumType checksumType;

    private FrameEncoderCrc(ChecksumType checksumType)
    {
        this.checksumType = checksumType;
    }

    public PayloadAllocator allocator()
    {
        return allocator;
    }

    /**
     * CASSANDRA-16360: entry point for callers to select a checksum-type-specific encoder singleton,
     * without touching the existing {@link #instance} field that native protocol and internode callers
     * already reference directly for the CRC32 case.
     */
    public static FrameEncoderCrc getInstance(ChecksumType checksumType)
    {
        switch (checksumType)
        {
            case CRC32:  return instance;
            case CRC32C: return crc32cInstance;
            default:     throw new UnsupportedOperationException(checksumType + " frame payload checksums are not implemented");
        }
    }

    static void writeHeader(ByteBuffer frame, boolean isSelfContained, int dataLength)
    {
        int header3b = dataLength;
        if (isSelfContained)
            header3b |= 1 << 17;
        int crc = crc24(header3b, 3);
        put3b(frame, 0, header3b);
        put3b(frame, 3, crc);
    }

    private static void put3b(ByteBuffer frame, int index, int put3b)
    {
        frame.put(index    , (byte) put3b        );
        frame.put(index + 1, (byte)(put3b >>> 8) );
        frame.put(index + 2, (byte)(put3b >>> 16));
    }

    ByteBuf encode(boolean isSelfContained, ByteBuffer frame)
    {
        try
        {
            int frameLength = frame.remaining();
            int dataLength = frameLength - HEADER_AND_TRAILER_LENGTH;
            if (dataLength >= 1 << 17)
                throw new IllegalArgumentException("Maximum payload size is 128KiB");

            writeHeader(frame, isSelfContained, dataLength);

            frame.position(HEADER_LENGTH);
            frame.limit(dataLength + HEADER_LENGTH);
            int frameCrc = (int) computePayloadChecksum(frame);

            if (frame.order() == ByteOrder.BIG_ENDIAN)
                frameCrc = Integer.reverseBytes(frameCrc);

            frame.limit(frameLength);
            frame.putInt(frameLength - TRAILER_LENGTH, frameCrc);
            frame.position(0);

            return GlobalBufferPoolAllocator.wrap(frame);
        }
        catch (Throwable t)
        {
            bufferPool.put(frame);
            throw t;
        }
    }

    /**
     * CASSANDRA-16360: the CRC32 path preserves {@link org.apache.cassandra.utils.Crc}'s exact
     * historical behavior, including its magic-prefix priming -- this is load-bearing for
     * byte-identical wire compatibility with every prior release. CRC32C intentionally does *not*
     * reuse that priming: it was CRC32-specific tuning with no established rationale for CRC32C
     * (see doc/modules/cassandra/pages/architecture/crc32c-plan.md §4.1b), so CRC32C frames are a
     * plain, unprimed checksum over the same payload bytes.
     */
    private long computePayloadChecksum(ByteBuffer payload)
    {
        if (checksumType == ChecksumType.CRC32)
        {
            CRC32 crc = crc32();
            crc.update(payload);
            return crc.getValue();
        }

        Checksum crc = checksumType.newInstance();
        crc.update(payload);
        return crc.getValue();
    }
}
