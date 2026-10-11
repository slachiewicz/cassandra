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
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;

import static org.apache.cassandra.net.OutboundConnectionsTest.LOCAL_ADDR;
import static org.apache.cassandra.net.OutboundConnectionsTest.REMOTE_ADDR;

/**
 * CASSANDRA-16360: the outbound {@code PayloadChecksum} gauge reports what the current connection to a peer
 * negotiated, and nothing while there is no connection. The negotiated values themselves are covered by
 * {@code ConnectionTest#testNegotiatedPayloadChecksum}.
 */
public class InternodeOutboundMetricsTest
{
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

    @After
    public void teardown()
    {
        if (metrics != null)
            metrics.release();
        metrics = null;
    }

    @Test
    public void payloadChecksumGauge_nullWhileNotConnected()
    {
        OutboundConnections connections = OutboundConnections.unsafeCreate(new OutboundConnectionSettings(REMOTE_ADDR));
        metrics = new InternodeOutboundMetrics(REMOTE_ADDR, connections);
        Assert.assertNull(metrics.payloadChecksum.getValue());
    }
}
