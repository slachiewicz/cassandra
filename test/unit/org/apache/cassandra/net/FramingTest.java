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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPools;
import org.apache.cassandra.utils.vint.VIntCoding;

import io.netty.buffer.ByteBuf;

import static java.lang.Math.min;
import static org.apache.cassandra.net.ShareableBytes.wrap;

// TODO: test corruption
// TODO: use a different random seed each time
// TODO: use quick theories
public class FramingTest
{
    private static final Logger logger = LoggerFactory.getLogger(FramingTest.class);

    @BeforeClass
    public static void begin() throws NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
        Verb._TEST_1.unsafeSetSerializer(() -> new IVersionedSerializer<byte[]>()
        {

            public void serialize(byte[] t, DataOutputPlus out, int version) throws IOException
            {
                out.writeUnsignedVInt32(t.length);
                out.write(t);
            }

            public byte[] deserialize(DataInputPlus in, int version) throws IOException
            {
                byte[] r = new byte[in.readUnsignedVInt32()];
                in.readFully(r);
                return r;
            }

            public long serializedSize(byte[] t, int version)
            {
                return VIntCoding.computeUnsignedVIntSize(t.length) + t.length;
            }
        });
    }

    @AfterClass
    public static void after() throws NoSuchFieldException, IllegalAccessException
    {
        Verb._TEST_1.unsafeSetSerializer(() -> null);
    }

    private static class SequenceOfFrames
    {
        final List<byte[]> original;
        final int[] boundaries;
        final ShareableBytes frames;

        private SequenceOfFrames(List<byte[]> original, int[] boundaries, ByteBuffer frames)
        {
            this.original = original;
            this.boundaries = boundaries;
            this.frames = wrap(frames);
        }
    }

    @Test
    public void testRandomLZ4()
    {
        testSomeFrames(FrameEncoderLZ4.fastInstance, FrameDecoderLZ4.fast(GlobalBufferPoolAllocator.instance));
    }

    @Test
    public void testRandomCrc()
    {
        testSomeFrames(FrameEncoderCrc.instance, FrameDecoderCrc.create(GlobalBufferPoolAllocator.instance));
    }

    // CASSANDRA-16360
    @Test
    public void testRandomCrc32c()
    {
        testSomeFrames(FrameEncoderCrc.getInstance(org.apache.cassandra.utils.ChecksumType.CRC32C),
                       FrameDecoderCrc.create(GlobalBufferPoolAllocator.instance, org.apache.cassandra.utils.ChecksumType.CRC32C));
    }

    // CASSANDRA-16360: a CRC32C-framed payload corrupted after encoding must be caught as a
    // recoverable checksum failure, the same as the existing CRC32 path -- not silently accepted.
    @Test
    public void testCorruptCrc32cFrameIsDetected()
    {
        FrameEncoder encoder = FrameEncoderCrc.getInstance(org.apache.cassandra.utils.ChecksumType.CRC32C);
        FrameDecoder decoder = FrameDecoderCrc.create(GlobalBufferPoolAllocator.instance, org.apache.cassandra.utils.ChecksumType.CRC32C);

        byte[] bytes = randomishBytes(new Random(1), 64, 128);
        FrameEncoder.Payload payload = encoder.allocator().allocate(true, bytes.length);
        payload.buffer.put(bytes);
        payload.finish();

        ByteBuf encoded = encoder.encode(true, payload.buffer);
        // flip one bit in the middle of the payload, well clear of the header and trailer
        int corruptIndex = FrameEncoderCrc.HEADER_LENGTH + bytes.length / 2;
        encoded.setByte(corruptIndex, encoded.getByte(corruptIndex) ^ 0x01);

        ByteBuffer frame = BufferPools.forNetworking().getAtLeast(encoded.readableBytes(), BufferType.OFF_HEAP);
        frame.put(encoded.internalNioBuffer(encoded.readerIndex(), encoded.readableBytes()));
        encoded.release();
        frame.flip();

        List<FrameDecoder.Frame> out = new ArrayList<>();
        ShareableBytes shareable = wrap(frame);
        decoder.decode(out, shareable);

        Assert.assertEquals(1, out.size());
        Assert.assertTrue("expected a CorruptFrame for a bit-flipped CRC32C payload", out.get(0) instanceof FrameDecoder.CorruptFrame);
        out.get(0).release();
    }

    // CASSANDRA-16360: known-answer tests pinning the on-wire payload-checksum trailer, independent
    // of the encoder's own implementation. "123456789" is the standard CRC check-value input: the
    // CRC32C (Castagnoli) trailer must be its published check value 0xE3069283 -- proving CRC32C
    // frames are UNprimed -- and the CRC32 trailer must equal a plain java.util.zip.CRC32 fed
    // Crc's magic prefix {0xFA,0x2D,0x55,0xCA} and then the payload -- proving the historical
    // priming survives byte-for-byte. Either assertion failing means the wire format changed and
    // peers on other versions can no longer validate our frames.
    @Test
    public void testKnownAnswerTrailers()
    {
        byte[] payload = "123456789".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

        Assert.assertEquals(0xE3069283, encodedPayloadTrailer(FrameEncoderCrc.getInstance(org.apache.cassandra.utils.ChecksumType.CRC32C), payload));

        java.util.zip.CRC32 primed = new java.util.zip.CRC32();
        primed.update(new byte[] { (byte) 0xFA, (byte) 0x2D, (byte) 0x55, (byte) 0xCA });
        primed.update(payload, 0, payload.length);
        Assert.assertEquals((int) primed.getValue(), encodedPayloadTrailer(FrameEncoderCrc.instance, payload));
    }

    /** Encodes {@code bytes} as a single self-contained frame and returns its little-endian trailer int. */
    private static int encodedPayloadTrailer(FrameEncoder encoder, byte[] bytes)
    {
        FrameEncoder.Payload payload = encoder.allocator().allocate(true, bytes.length);
        payload.buffer.put(bytes);
        payload.finish();
        ByteBuf encoded = encoder.encode(true, payload.buffer);
        try
        {
            int end = encoded.readableBytes();
            return (encoded.getByte(end - 4) & 0xFF)
                   | (encoded.getByte(end - 3) & 0xFF) << 8
                   | (encoded.getByte(end - 2) & 0xFF) << 16
                   | (encoded.getByte(end - 1) & 0xFF) << 24;
        }
        finally
        {
            encoded.release();
        }
    }

    // CASSANDRA-16360: an unrecognized framing id must raise a specific, clean exception rather than
    // a bare IllegalStateException, so it flows through the normal handshake-decode error handling
    // (see OutboundConnectionSettings.Framing.UnknownFramingException).
    @Test
    public void testUnknownFramingIdRejectedCleanly()
    {
        for (int id : new int[]{ 4, 5, 255 })
        {
            try
            {
                OutboundConnectionSettings.Framing.forId(id);
                Assert.fail("expected UnknownFramingException for framing id " + id);
            }
            catch (OutboundConnectionSettings.Framing.UnknownFramingException e)
            {
                Assert.assertEquals(id, e.id);
            }
        }
    }

    private void testSomeFrames(FrameEncoder encoder, FrameDecoder decoder)
    {
        long seed = new SecureRandom().nextLong();
        logger.info("seed: {}, decoder: {}", seed, decoder.getClass().getSimpleName());
        Random random = new Random(seed);
        for (int i = 0 ; i < 1000 ; ++i)
            testRandomSequenceOfFrames(random, encoder, decoder);
    }

    private void testRandomSequenceOfFrames(Random random, FrameEncoder encoder, FrameDecoder decoder)
    {
        SequenceOfFrames sequenceOfFrames = sequenceOfFrames(random, encoder);

        List<byte[]> uncompressed = sequenceOfFrames.original;
        ShareableBytes frames = sequenceOfFrames.frames;
        int[] boundaries = sequenceOfFrames.boundaries;

        int end = frames.get().limit();
        List<FrameDecoder.Frame> out = new ArrayList<>();
        int prevBoundary = -1;
        for (int i = 0 ; i < end ; )
        {
            int limit = i + random.nextInt(1 + end - i);
            decoder.decode(out, frames.slice(i, limit));
            int boundary = Arrays.binarySearch(boundaries, limit);
            if (boundary < 0) boundary = -2 -boundary;

            while (prevBoundary < boundary)
            {
                ++prevBoundary;
                Assert.assertTrue(out.size() >= 1 + prevBoundary);
                verify(uncompressed.get(prevBoundary), ((FrameDecoder.IntactFrame) out.get(prevBoundary)).contents);
            }
            i = limit;
        }
        for (FrameDecoder.Frame frame : out)
            frame.release();
        frames.release();
        Assert.assertNull(decoder.stash);
        Assert.assertTrue(decoder.frames.isEmpty());
    }

    private static void verify(byte[] expect, ShareableBytes actual)
    {
        verify(expect, 0, expect.length, actual);
    }

    private static void verify(byte[] expect, int start, int end, ShareableBytes actual)
    {
        byte[] fetch = new byte[end - start];
        Assert.assertEquals(end - start, actual.remaining());
        actual.get().get(fetch);
        boolean equals = true;
        for (int i = start ; equals && i < end ; ++i)
            equals = expect[i] == fetch[i - start];
        if (!equals)
            Assert.assertArrayEquals(Arrays.copyOfRange(expect, start, end), fetch);
    }

    private static SequenceOfFrames sequenceOfFrames(Random random, FrameEncoder encoder)
    {
        int frameCount = 1 + random.nextInt(8);
        List<byte[]> uncompressed = new ArrayList<>();
        List<ByteBuf> compressed = new ArrayList<>();
        int[] cumulativeCompressedLength = new int[frameCount];
        for (int i = 0 ; i < frameCount ; ++i)
        {
            byte[] bytes = randomishBytes(random, 1, 1 << 15);
            uncompressed.add(bytes);

            FrameEncoder.Payload payload = encoder.allocator().allocate(true, bytes.length);
            payload.buffer.put(bytes);
            payload.finish();

            ByteBuf buffer = encoder.encode(true, payload.buffer);
            compressed.add(buffer);
            cumulativeCompressedLength[i] = (i == 0 ? 0 : cumulativeCompressedLength[i - 1]) + buffer.readableBytes();
        }

        ByteBuffer frames = BufferPools.forNetworking().getAtLeast(cumulativeCompressedLength[frameCount - 1], BufferType.OFF_HEAP);
        for (ByteBuf buffer : compressed)
        {
            frames.put(buffer.internalNioBuffer(buffer.readerIndex(), buffer.readableBytes()));
            buffer.release();
        }
        frames.flip();
        return new SequenceOfFrames(uncompressed, cumulativeCompressedLength, frames);
    }

    @Test
    public void testSerializeSizeMatchesEdgeCases() // See CASSANDRA-16103
    {
        int v40 = MessagingService.Version.VERSION_40.value;
        Consumer<Long> subTest = timeGapInMillis ->
        {
            long createdAt = 0;
            long expiresAt = createdAt + TimeUnit.MILLISECONDS.toNanos(timeGapInMillis);
            Message<NoPayload> message = Message.builder(Verb.READ_REPAIR_RSP, NoPayload.noPayload)
                                                .from(FBUtilities.getBroadcastAddressAndPort())
                                                .withCreatedAt(createdAt)
                                                .withExpiresAt(expiresAt)
                                                .build();

            try (DataOutputBuffer out = new DataOutputBuffer(20))
            {
                Message.serializer.serialize(message, out, v40);
                Assert.assertEquals(message.serializedSize(v40), out.getLength());
            }
            catch (IOException ioe)
            {
                Assert.fail("Unexpected IOEception during test. " + ioe.getMessage());
            }
        };

        // test cases
        subTest.accept(-1L);
        subTest.accept(1L << 7 - 1);
        subTest.accept(1L << 14 - 1);
    }

    public static byte[] randomishBytes(Random random, int minLength, int maxLength)
    {
        byte[] bytes = new byte[minLength + random.nextInt(Math.max(1, maxLength - minLength))];
        int runLength = 1 + random.nextInt(255);
        for (int i = 0 ; i < bytes.length ; i += runLength)
        {
            byte b = (byte) random.nextInt(256);
            Arrays.fill(bytes, i, min(bytes.length, i + runLength), b);
        }
        return bytes;
    }

}
