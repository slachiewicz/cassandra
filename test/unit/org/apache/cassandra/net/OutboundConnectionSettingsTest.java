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

import java.util.function.Function;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.net.OutboundConnectionsTest.LOCAL_ADDR;
import static org.apache.cassandra.net.OutboundConnectionsTest.REMOTE_ADDR;

public class OutboundConnectionSettingsTest
{
    private static final String DC1 = "dc1";
    private static final String DC2 = "dc2";
    private static final String RACK = "rack1";

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Before
    public void reset()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_SmallSendSize()
    {
        test(settings -> settings.withSocketSendBufferSizeInBytes(999));
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_SendSizeLessThanZero()
    {
        test(settings -> settings.withSocketSendBufferSizeInBytes(-1));
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_TcpConnectTimeoutLessThanZero()
    {
        test(settings -> settings.withTcpConnectTimeoutInMS(-1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void build_TcpUserTimeoutLessThanZero()
    {
        test(settings -> settings.withTcpUserTimeoutInMS(-1));
    }

    @Test
    public void build_TcpUserTimeoutEqualsZero()
    {
        test(settings -> settings.withTcpUserTimeoutInMS(0));
    }

    private static void test(Function<OutboundConnectionSettings, OutboundConnectionSettings> f)
    {
        f.apply(new OutboundConnectionSettings(LOCAL_ADDR)).withDefaults(ConnectionCategory.MESSAGING);
    }

    @Test
    public void shouldCompressConnection_None()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.none);
        Assert.assertFalse(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_DifferentDc()
    {
        ClusterMetadataTestHelper.register(LOCAL_ADDR, DC1, RACK);
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC2, RACK);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertTrue(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_MetadataNotInitialized()
    {
        // if cluster metadata isn't yet available then we assume that every peer is remote.
        // connections will be re-established once the cluster metadata service becomes available
        ClusterMetadataTestHelper.register(LOCAL_ADDR, DC1, RACK);
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC2, RACK);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertTrue(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_All()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.all);
        Assert.assertTrue(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_SameDc()
    {
        ClusterMetadataTestHelper.register(LOCAL_ADDR, DC1, RACK);
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC1, RACK);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertFalse(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    // CASSANDRA-16360: with the peer registered at a version that can decode CRC32C framing,
    // Framing.CRC32C is chosen only for (crc32c, NONE); every other combination of
    // internode_checksum_type x storage_compatibility_mode falls back to plain CRC.
    @Test
    public void framing_Crc32cOnlyWhenOptedInAndCompatibilityModeNone()
    {
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC1, RACK);
        withChecksumSettings(() -> {
            assertFraming(Config.InternodeChecksumType.crc32, StorageCompatibilityMode.NONE, OutboundConnectionSettings.Framing.CRC);
            assertFraming(Config.InternodeChecksumType.crc32, StorageCompatibilityMode.UPGRADING, OutboundConnectionSettings.Framing.CRC);
            assertFraming(Config.InternodeChecksumType.crc32c, StorageCompatibilityMode.NONE, OutboundConnectionSettings.Framing.CRC32C);
            assertFraming(Config.InternodeChecksumType.crc32c, StorageCompatibilityMode.UPGRADING, OutboundConnectionSettings.Framing.CRC);
            assertFraming(Config.InternodeChecksumType.crc32c, StorageCompatibilityMode.CASSANDRA_5, OutboundConnectionSettings.Framing.CRC);
        });
    }

    // CASSANDRA-16360: storage_compatibility_mode NONE is the default and says nothing about minor
    // versions, so the peer's own release version decides. A 6.0 peer cannot decode framing id 3.
    @Test
    public void framing_Crc32cNotOfferedToPeerOnOlderRelease()
    {
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC1, RACK, NodeVersion.fromCassandraVersion(new CassandraVersion("6.0.1")));
        withChecksumSettings(() -> assertFraming(Config.InternodeChecksumType.crc32c, StorageCompatibilityMode.NONE, OutboundConnectionSettings.Framing.CRC));
    }

    @Test
    public void framing_Crc32cNotOfferedToUnregisteredPeer()
    {
        withChecksumSettings(() -> assertFraming(Config.InternodeChecksumType.crc32c, StorageCompatibilityMode.NONE, OutboundConnectionSettings.Framing.CRC));
    }

    @Test
    public void supportsCrc32cFraming_ByReleaseVersion()
    {
        Assert.assertFalse(OutboundConnectionSettings.supportsCrc32cFraming(null));
        Assert.assertFalse(OutboundConnectionSettings.supportsCrc32cFraming(new CassandraVersion("5.0.5")));
        Assert.assertFalse(OutboundConnectionSettings.supportsCrc32cFraming(new CassandraVersion("6.0.0")));
        Assert.assertFalse(OutboundConnectionSettings.supportsCrc32cFraming(new CassandraVersion("6.1.3")));
        Assert.assertTrue(OutboundConnectionSettings.supportsCrc32cFraming(new CassandraVersion("7.0-alpha1")));
        Assert.assertTrue(OutboundConnectionSettings.supportsCrc32cFraming(new CassandraVersion("7.0-SNAPSHOT")));
        Assert.assertTrue(OutboundConnectionSettings.supportsCrc32cFraming(new CassandraVersion("7.0.0")));
        Assert.assertTrue(OutboundConnectionSettings.supportsCrc32cFraming(new CassandraVersion("8.0.0")));
    }

    // CASSANDRA-16360: a pool that chose CRC32C keeps that framing for its lifetime. If the peer later
    // stops qualifying (binary downgraded under the same address), reconnect attempts must fall back to
    // CRC rather than send framing id 3 to a node that rejects it on every handshake.
    @Test
    public void withSafeFraming_FallsBackWhenPeerNoLongerQualifies()
    {
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC1, RACK, NodeVersion.fromCassandraVersion(new CassandraVersion("6.0.1")));
        withChecksumSettings(() -> {
            DatabaseDescriptor.setInternodeChecksumType(Config.InternodeChecksumType.crc32c);
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
            OutboundConnectionSettings settings = new OutboundConnectionSettings(REMOTE_ADDR).withFraming(OutboundConnectionSettings.Framing.CRC32C);
            Assert.assertEquals(OutboundConnectionSettings.Framing.CRC, settings.withSafeFraming().framing);

            OutboundConnectionSettings crc = new OutboundConnectionSettings(REMOTE_ADDR).withFraming(OutboundConnectionSettings.Framing.CRC);
            Assert.assertSame(crc, crc.withSafeFraming());
        });
    }

    @Test
    public void withSafeFraming_KeepsCrc32cWhilePeerQualifies()
    {
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC1, RACK);
        withChecksumSettings(() -> {
            DatabaseDescriptor.setInternodeChecksumType(Config.InternodeChecksumType.crc32c);
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
            OutboundConnectionSettings settings = new OutboundConnectionSettings(REMOTE_ADDR).withFraming(OutboundConnectionSettings.Framing.CRC32C);
            Assert.assertSame(settings, settings.withSafeFraming());
        });
    }

    private static void withChecksumSettings(Runnable test)
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.none);
        StorageCompatibilityMode originalMode = DatabaseDescriptor.getStorageCompatibilityMode();
        Config.InternodeChecksumType originalChecksumType = DatabaseDescriptor.internodeChecksumType();
        try
        {
            test.run();
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(originalMode);
            DatabaseDescriptor.setInternodeChecksumType(originalChecksumType);
        }
    }

    private static void assertFraming(Config.InternodeChecksumType checksumType, StorageCompatibilityMode mode, OutboundConnectionSettings.Framing expected)
    {
        DatabaseDescriptor.setInternodeChecksumType(checksumType);
        DatabaseDescriptor.setStorageCompatibilityMode(mode);
        OutboundConnectionSettings settings = new OutboundConnectionSettings(REMOTE_ADDR);
        Assert.assertEquals("checksumType=" + checksumType + " mode=" + mode,
                            expected, settings.framing(ConnectionCategory.MESSAGING));
    }
}
