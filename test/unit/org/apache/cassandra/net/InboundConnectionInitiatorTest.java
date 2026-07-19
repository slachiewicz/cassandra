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
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-16360: {@link OutboundConnectionSettings.Framing.UnknownFramingException} (a peer sending an
 * internode framing id this node doesn't recognize -- e.g. a stale peer, or an
 * internode_checksum_type/storage_compatibility_mode mismatch) must be logged distinctly from a real
 * {@link org.apache.cassandra.utils.Crc.InvalidCrc} corruption event, so an operator can tell "misconfiguration"
 * from "possible data corruption" from logs alone. See crc32c-plan.md §4.6a.
 */
public class InboundConnectionInitiatorTest
{
    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        logger = (Logger) LoggerFactory.getLogger(InboundConnectionInitiator.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @After
    public void teardown()
    {
        logger.detachAppender(appender);
    }

    private static TestChannel newChannelWithHandler()
    {
        TestChannel channel = new TestChannel();
        channel.pipeline().addLast(new InboundConnectionInitiator.Handler(new InboundConnectionSettings()));
        return channel;
    }

    @Test
    public void unknownFramingException_logsDistinctWarnMessage_notGenericCorruptionError()
    {
        TestChannel channel = newChannelWithHandler();
        channel.pipeline().fireExceptionCaught(new OutboundConnectionSettings.Framing.UnknownFramingException(3));

        List<ILoggingEvent> events = appender.list;
        assertEquals(1, events.size());

        ILoggingEvent event = events.get(0);
        assertEquals(Level.WARN, event.getLevel());

        String message = event.getFormattedMessage();
        assertTrue(message, message.contains("unrecognized internode framing id"));
        assertTrue(message, message.contains("3"));
        assertTrue(message, message.contains("not data corruption"));
        assertFalse(message, message.contains("Failed to properly handshake"));
    }

    @Test
    public void genericIOException_stillLogsGenericHandshakeFailureAtError()
    {
        TestChannel channel = newChannelWithHandler();
        channel.pipeline().fireExceptionCaught(new IOException("boom"));

        List<ILoggingEvent> events = appender.list;
        assertEquals(1, events.size());

        ILoggingEvent event = events.get(0);
        assertEquals(Level.ERROR, event.getLevel());
        assertTrue(event.getFormattedMessage(), event.getFormattedMessage().contains("Failed to properly handshake"));
    }

    @Test
    public void unknownFramingExceptionWrappedInAnotherException_isStillClassifiedByItsRootCause()
    {
        TestChannel channel = newChannelWithHandler();
        channel.pipeline().fireExceptionCaught(new IOException("decode failed", new OutboundConnectionSettings.Framing.UnknownFramingException(3)));

        List<ILoggingEvent> events = appender.list;
        assertEquals(1, events.size());

        ILoggingEvent event = events.get(0);
        assertEquals(Level.WARN, event.getLevel());
        assertTrue(event.getFormattedMessage(), event.getFormattedMessage().contains("unrecognized internode framing id"));
    }
}
