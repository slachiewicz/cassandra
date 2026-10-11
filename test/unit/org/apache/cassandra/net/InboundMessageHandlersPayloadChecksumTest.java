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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.InternodeInboundMetrics;
import org.apache.cassandra.utils.ChecksumType;

import static org.apache.cassandra.net.OutboundConnectionsTest.LOCAL_ADDR;
import static org.apache.cassandra.net.OutboundConnectionsTest.REMOTE_ADDR;

/**
 * CASSANDRA-16360: the payload checksum negotiated by a peer's inbound connection must be observable per peer,
 * so operators and the mixed-version tests described in crc32c-plan.md §4.4 can see whether CRC32C is in use.
 */
public class InboundMessageHandlersPayloadChecksumTest
{
    private InboundMessageHandlers handlers;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void teardown()
    {
        if (handlers != null)
            handlers.releaseMetrics();
    }

    private InboundMessageHandlers newHandlers()
    {
        InboundMessageHandlers.GlobalResourceLimits globalLimits =
            new InboundMessageHandlers.GlobalResourceLimits(new ResourceLimits.Concurrent(1024 * 1024));

        InboundMessageHandlers.GlobalMetricCallbacks noopGlobalMetrics = new InboundMessageHandlers.GlobalMetricCallbacks()
        {
            public LatencyConsumer internodeLatencyRecorder(InetAddressAndPort to)
            {
                return (timeElapsed, unit) -> {};
            }

            public void recordInternalLatency(Verb verb, long timeElapsed, TimeUnit timeUnit) {}
            public void recordInternodeDroppedMessage(Verb verb, long timeElapsed, TimeUnit timeUnit) {}
        };

        InboundMessageHandlers.MessageConsumer noopConsumer = new InboundMessageHandlers.MessageConsumer()
        {
            public void accept(Message<?> message) {}
            public void fail(Message.Header header, Throwable failure) {}
        };

        // handlerProvider is never exercised by these tests: createHandler() records the payload checksum
        // *before* delegating to it, so a stub that returns null is sufficient and avoids needing a real
        // Channel/FrameDecoder.
        InboundMessageHandlers.HandlerProvider stubProvider =
            (decoder, type, channel, self, peer, version, largeMessageThreshold,
             queueCapacity, endpointReserveCapacity, globalReserveCapacity,
             endpointWaitQueue, globalWaitQueue, onClosed, callbacks, consumer) -> null;

        return new InboundMessageHandlers(LOCAL_ADDR, REMOTE_ADDR, 1024, 1024 * 1024,
                                           globalLimits, noopGlobalMetrics, noopConsumer, stubProvider);
    }

    @SuppressWarnings("unchecked")
    private static String readPayloadChecksumGauge(InetAddressAndPort peer)
    {
        String metricName = new DefaultNameFactory(InternodeInboundMetrics.TYPE_NAME, peer.getHostAddressAndPortForJMX())
                             .createMetricName("PayloadChecksum")
                             .getMetricName();
        Gauge<String> gauge = (Gauge<String>) CassandraMetricsRegistry.Metrics.getMetrics().get(metricName);
        Assert.assertNotNull("PayloadChecksum gauge not registered for " + peer, gauge);
        return gauge.getValue();
    }

    @Test
    public void noConnectionYet_currentPayloadChecksumIsNull()
    {
        handlers = newHandlers();
        Assert.assertNull(handlers.currentPayloadChecksumName());
    }

    @Test
    public void createHandler_recordsNegotiatedChecksum()
    {
        handlers = newHandlers();

        handlers.createHandler(null, ChecksumType.CRC32C, ConnectionType.SMALL_MESSAGES, null, MessagingService.current_version);
        Assert.assertEquals("CRC32C", handlers.currentPayloadChecksumName());
        Assert.assertEquals("CRC32C", readPayloadChecksumGauge(REMOTE_ADDR));
    }

    @Test
    public void createHandler_reflectsMostRecentConnectionOnReconnect()
    {
        handlers = newHandlers();

        handlers.createHandler(null, ChecksumType.CRC32, ConnectionType.SMALL_MESSAGES, null, MessagingService.current_version);
        Assert.assertEquals("CRC32", handlers.currentPayloadChecksumName());
        Assert.assertEquals("CRC32", readPayloadChecksumGauge(REMOTE_ADDR));

        // simulate a reconnect that negotiates a different checksum (e.g. after the peer was upgraded)
        handlers.createHandler(null, ChecksumType.CRC32C, ConnectionType.SMALL_MESSAGES, null, MessagingService.current_version);
        Assert.assertEquals("CRC32C", handlers.currentPayloadChecksumName());
        Assert.assertEquals("CRC32C", readPayloadChecksumGauge(REMOTE_ADDR));
    }

    @Test
    public void payloadChecksumGauge_removedOnRelease()
    {
        handlers = newHandlers();
        handlers.createHandler(null, ChecksumType.CRC32C, ConnectionType.SMALL_MESSAGES, null, MessagingService.current_version);
        Assert.assertNotNull(readPayloadChecksumGauge(REMOTE_ADDR));

        handlers.releaseMetrics();
        handlers = null; // already released, don't release again in teardown

        String metricName = new DefaultNameFactory(InternodeInboundMetrics.TYPE_NAME, REMOTE_ADDR.getHostAddressAndPortForJMX())
                             .createMetricName("PayloadChecksum")
                             .getMetricName();
        Assert.assertNull(CassandraMetricsRegistry.Metrics.getMetrics().get(metricName));
    }
}
