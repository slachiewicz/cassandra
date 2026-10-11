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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.HandshakeProtocol.Accept;
import org.apache.cassandra.net.HandshakeProtocol.Initiate;
import org.apache.cassandra.net.OutboundConnectionSettings.Framing;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.apache.cassandra.net.OutboundConnectionsTest.LOCAL_ADDR;
import static org.apache.cassandra.utils.Crc.computeCrc32;

/**
 * CASSANDRA-16360: wire format of the CRC32C request (bit 5 of the {@link Initiate} flags) and
 * acknowledgement (bit 30 of the first {@link Accept} int). Messages that don't use them must stay
 * byte-identical to earlier releases, and the bits must not disturb any field an older release reads.
 */
public class HandshakeProtocolCrc32cTest
{
    private static final AcceptVersions VERSIONS = new AcceptVersions(minimum_version, current_version);

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static int flags(Initiate initiate)
    {
        ByteBuf encoded = initiate.encode();
        try
        {
            return encoded.getInt(encoded.readerIndex() + 4); // after PROTOCOL_MAGIC
        }
        finally
        {
            encoded.release();
        }
    }

    @Test
    public void initiateRequestIsOnlyBit5()
    {
        for (Framing framing : Framing.values())
        {
            int without = flags(new Initiate(VERSIONS, ConnectionType.SMALL_MESSAGES, framing, false, LOCAL_ADDR));
            int with = flags(new Initiate(VERSIONS, ConnectionType.SMALL_MESSAGES, framing, true, LOCAL_ADDR));
            Assert.assertEquals(framing.toString(), 1 << 5, with ^ without);
            // the original four-argument constructor still produces the pre-CASSANDRA-16360 encoding
            Assert.assertEquals(without, flags(new Initiate(VERSIONS, ConnectionType.SMALL_MESSAGES, framing, LOCAL_ADDR)));
        }
    }

    @Test
    public void initiateRoundTrip() throws Exception
    {
        for (boolean request : new boolean[]{ false, true })
        {
            Initiate sent = new Initiate(VERSIONS, ConnectionType.LARGE_MESSAGES, Framing.LZ4, request, LOCAL_ADDR);
            ByteBuf encoded = sent.encode();
            try
            {
                Initiate received = Initiate.maybeDecode(encoded);
                Assert.assertEquals(sent, received);
                Assert.assertEquals(request, received.requestsCrc32c);
                Assert.assertEquals(Framing.LZ4, received.framing);
                Assert.assertEquals(ConnectionType.LARGE_MESSAGES, received.type);
            }
            finally
            {
                encoded.release();
            }
        }
    }

    @Test
    public void acceptWithoutAckIsByteIdenticalToEarlierReleases()
    {
        ByteBuf encoded = new Accept(current_version, current_version, false).encode(ByteBufAllocator.DEFAULT);
        try
        {
            Assert.assertEquals(current_version, encoded.getInt(0));
            Assert.assertEquals(current_version, encoded.getInt(4));
            Assert.assertEquals(computeCrc32(encoded, 0, 8), encoded.getInt(8));
        }
        finally
        {
            encoded.release();
        }
    }

    @Test
    public void acceptAckIsOnlyBit30AndRoundTrips() throws Exception
    {
        for (boolean ack : new boolean[]{ false, true })
        {
            Accept sent = new Accept(current_version, current_version, ack);
            ByteBuf encoded = sent.encode(ByteBufAllocator.DEFAULT);
            try
            {
                Assert.assertEquals(ack ? current_version | (1 << 30) : current_version, encoded.getInt(0));
                Accept received = Accept.maybeDecode(encoded);
                Assert.assertEquals(sent, received);
                Assert.assertEquals(ack, received.crc32cAccepted);
                Assert.assertEquals(current_version, received.maxMessagingVersion);
            }
            finally
            {
                encoded.release();
            }
        }
    }

    // an Accept from a release that predates CASSANDRA-16360, built byte by byte as that code wrote it
    @Test
    public void acceptFromEarlierReleaseMeansNoCrc32c() throws Exception
    {
        ByteBuf old = Unpooled.directBuffer(12);
        try
        {
            old.writeInt(current_version);
            old.writeInt(current_version);
            old.writeInt(computeCrc32(old, 0, 8));
            Accept received = Accept.maybeDecode(old);
            Assert.assertFalse(received.crc32cAccepted);
            Assert.assertEquals(current_version, received.maxMessagingVersion);
            Assert.assertEquals(current_version, received.useMessagingVersion);
        }
        finally
        {
            old.release();
        }
    }
}
