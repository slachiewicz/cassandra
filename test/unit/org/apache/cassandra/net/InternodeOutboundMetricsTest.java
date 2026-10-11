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
 * CASSANDRA-16360: the internode {@link OutboundConnectionSettings.Framing} used for a peer must be observable
 * per peer, so the mixed-version dtests described in crc32c-plan.md §4.4 have something concrete to assert on.
 * The gauge reports the framing the pool actually uses, which is fixed when the pool is created.
 */
public class InternodeOutboundMetricsTest
{
    private StorageCompatibilityMode originalMode;
    private Config.InternodeChecksumType originalChecksumType;
    private Config.InternodeCompression originalCompression;
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
    }

    @After
    public void teardown()
    {
        if (metrics != null)
            metrics.release();
        metrics = null;
        DatabaseDescriptor.setStorageCompatibilityMode(originalMode);
        DatabaseDescriptor.setInternodeChecksumType(originalChecksumType);
        DatabaseDescriptor.setInternodeCompression(originalCompression);
    }

    private String framingFor(Config.InternodeChecksumType checksumType, StorageCompatibilityMode mode)
    {
        DatabaseDescriptor.setInternodeChecksumType(checksumType);
        DatabaseDescriptor.setStorageCompatibilityMode(mode);
        OutboundConnections connections = OutboundConnections.unsafeCreate(new OutboundConnectionSettings(REMOTE_ADDR));
        metrics = new InternodeOutboundMetrics(REMOTE_ADDR, connections);
        return metrics.framing.getValue();
    }

    @Test
    public void framingGauge_reportsCrc32ByDefault()
    {
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC.name(),
                            framingFor(Config.InternodeChecksumType.crc32, StorageCompatibilityMode.NONE));
    }

    @Test
    public void framingGauge_reportsCrc32cWhenOptedInAndCompatibilityModeNone()
    {
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC32C.name(),
                            framingFor(Config.InternodeChecksumType.crc32c, StorageCompatibilityMode.NONE));
    }

    @Test
    public void framingGauge_fallsBackToCrc32WhileUpgrading()
    {
        // even with crc32c opted in, UPGRADING must never select it (§4.1b)
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC.name(),
                            framingFor(Config.InternodeChecksumType.crc32c, StorageCompatibilityMode.UPGRADING));
    }

    @Test
    public void framingGauge_reportsPoolFramingNotCurrentConfig()
    {
        // framing is fixed when the pool is created; a later config change must not make the gauge
        // report a framing the pool is not using (§4.5: changing the checksum type needs a restart)
        String framing = framingFor(Config.InternodeChecksumType.crc32, StorageCompatibilityMode.NONE);
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC.name(), framing);

        DatabaseDescriptor.setInternodeChecksumType(Config.InternodeChecksumType.crc32c);
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC.name(), metrics.framing.getValue());
    }

    @Test
    public void framingGauge_reportsFallbackWhenCrc32cNoLongerPermitted()
    {
        // a pool created with CRC32C reports CRC once the next attempt would fall back (§4.5)
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC32C.name(),
                            framingFor(Config.InternodeChecksumType.crc32c, StorageCompatibilityMode.NONE));

        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.UPGRADING);
        Assert.assertEquals(OutboundConnectionSettings.Framing.CRC.name(), metrics.framing.getValue());
    }
}
