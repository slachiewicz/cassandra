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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.net.OutboundConnectionsTest.LOCAL_ADDR;
import static org.apache.cassandra.net.OutboundConnectionsTest.REMOTE_ADDR;

/**
 * CASSANDRA-16360: the negotiated internode {@link OutboundConnectionSettings.Framing} for a peer must be
 * observable per-peer (not just inferable from config), so the mixed-version/mixed-compatibility-mode dtests
 * described in crc32c-plan.md §4.4 have something concrete to assert on. See crc32c-plan.md §4.6a.
 */
public class InternodeOutboundMetricsTest
{
    private StorageCompatibilityMode originalMode;
    private Config.InternodeChecksumType originalChecksumType;
    private Config.InternodeCompression originalCompression;
    private OutboundConnections connections;
    private InternodeOutboundMetrics metrics;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
        ClusterMetadataTestHelper.register(LOCAL_ADDR);
        ClusterMetadataTestHelper.register(REMOTE_ADDR);
        CommitLog.instance.start();
    }

    @Before
    public void setup()
    {
        originalMode = DatabaseDescriptor.getStorageCompatibilityMode();
        originalChecksumType = DatabaseDescriptor.internodeChecksumType();
        originalCompression = DatabaseDescriptor.internodeCompression();
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.none);

        connections = OutboundConnections.unsafeCreate(new OutboundConnectionSettings(REMOTE_ADDR));
        metrics = new InternodeOutboundMetrics(REMOTE_ADDR, connections);
    }

    @After
    public void teardown()
    {
        metrics.release();
        DatabaseDescriptor.setStorageCompatibilityMode(originalMode);
        DatabaseDescriptor.setInternodeChecksumType(originalChecksumType);
        DatabaseDescriptor.setInternodeCompression(originalCompression);
    }

    @Test
    public void framingGauge_reportsCrc32ByDefault()
    {
        DatabaseDescriptor.setInternodeChecksumType(Config.InternodeChecksumType.crc32);
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);

        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC.name(), metrics.framing.getValue());
    }

    @Test
    public void framingGauge_reportsCrc32cWhenOptedInAndCompatibilityModeNone()
    {
        DatabaseDescriptor.setInternodeChecksumType(Config.InternodeChecksumType.crc32c);
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);

        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC32C.name(), metrics.framing.getValue());
    }

    @Test
    public void framingGauge_fallsBackToCrc32WhileUpgrading()
    {
        // even with crc32c opted in, a cluster mid-upgrade must never actually select it (§4.1b)
        DatabaseDescriptor.setInternodeChecksumType(Config.InternodeChecksumType.crc32c);
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.UPGRADING);

        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC.name(), metrics.framing.getValue());
    }

    @Test
    public void framingGauge_reflectsLiveConfigChanges_noReconnectRequired()
    {
        // the rollback story (§4.5) depends on framing() being re-evaluated on every read, not cached at
        // connection-pool-build time -- confirm the gauge (and thus the underlying framing() call) picks up
        // a config flip without recreating the OutboundConnections/metrics objects.
        DatabaseDescriptor.setInternodeChecksumType(Config.InternodeChecksumType.crc32c);
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC32C.name(), metrics.framing.getValue());

        DatabaseDescriptor.setInternodeChecksumType(Config.InternodeChecksumType.crc32);
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC.name(), metrics.framing.getValue());
    }
}
