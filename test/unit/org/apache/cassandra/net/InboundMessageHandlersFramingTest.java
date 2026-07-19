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

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Gauge;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.InternodeInboundMetrics;

import static org.apache.cassandra.net.OutboundConnectionsTest.LOCAL_ADDR;
import static org.apache.cassandra.net.OutboundConnectionsTest.REMOTE_ADDR;

/**
 * CASSANDRA-16360: the {@link OutboundConnectionSettings.Framing} actually being decoded from a peer's inbound
 * connection must be observable per-peer, distinct from (and comparable against) the outbound gauge, so a
 * mismatch between "what I sent" and "what arrived" is directly visible rather than only inferable from an
 * {@link OutboundConnectionSettings.Framing.UnknownFramingException} log line. See crc32c-plan.md §4.6a.
 */
public class InboundMessageHandlersFramingTest
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

        // handlerProvider is never exercised by these tests: createHandler() records the decoded Framing
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
    private static String readFramingGauge(InetAddressAndPort peer)
    {
        String metricName = new DefaultNameFactory(InternodeInboundMetrics.TYPE_NAME, peer.getHostAddressAndPortForJMX())
                             .createMetricName("Framing")
                             .getMetricName();
        Gauge<String> gauge = (Gauge<String>) CassandraMetricsRegistry.Metrics.getMetrics().get(metricName);
        Assert.assertNotNull("Framing gauge not registered for " + peer, gauge);
        return gauge.getValue();
    }

    @Test
    public void noConnectionYet_currentFramingNameIsNull()
    {
        handlers = newHandlers();
        Assert.assertNull(handlers.currentFramingName());
    }

    @Test
    public void createHandler_recordsDecodedFraming()
    {
        handlers = newHandlers();

        handlers.createHandler(null, OutboundConnectionSettings.Framing.CRC32C, ConnectionType.SMALL_MESSAGES, null, MessagingService.current_version);
        Assert.assertEquals("CRC32C", handlers.currentFramingName());
        Assert.assertEquals("CRC32C", readFramingGauge(REMOTE_ADDR));
    }

    @Test
    public void createHandler_reflectsMostRecentConnectionOnReconnect()
    {
        handlers = newHandlers();

        handlers.createHandler(null, OutboundConnectionSettings.Framing.CRC, ConnectionType.SMALL_MESSAGES, null, MessagingService.current_version);
        Assert.assertEquals("CRC", handlers.currentFramingName());
        Assert.assertEquals("CRC", readFramingGauge(REMOTE_ADDR));

        // simulate a reconnect that negotiates a different framing (e.g. after a config/compatibility-mode change)
        handlers.createHandler(null, OutboundConnectionSettings.Framing.CRC32C, ConnectionType.SMALL_MESSAGES, null, MessagingService.current_version);
        Assert.assertEquals("CRC32C", handlers.currentFramingName());
        Assert.assertEquals("CRC32C", readFramingGauge(REMOTE_ADDR));
    }

    @Test
    public void framingGauge_removedOnRelease()
    {
        handlers = newHandlers();
        handlers.createHandler(null, OutboundConnectionSettings.Framing.CRC32C, ConnectionType.SMALL_MESSAGES, null, MessagingService.current_version);
        Assert.assertNotNull(readFramingGauge(REMOTE_ADDR));

        handlers.releaseMetrics();
        handlers = null; // already released, don't release again in teardown

        String metricName = new DefaultNameFactory(InternodeInboundMetrics.TYPE_NAME, REMOTE_ADDR.getHostAddressAndPortForJMX())
                             .createMetricName("Framing")
                             .getMetricName();
        Assert.assertNull(CassandraMetricsRegistry.Metrics.getMetrics().get(metricName));
    }
}
