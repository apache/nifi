/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.nifi.spring.SpringDataExchanger.SpringResponse;
import org.junit.Test;

public class SpringContextFactoryTests {

    @Test(expected = IllegalStateException.class)
    public void validateACFailureDueToNotFound() {
        SpringContextFactory.createSpringContextDelegate(".", "foo.xml");
    }

    @Test(expected = IllegalStateException.class)
    public void validateMessageNotSentNoFromNiFiChannel() throws Exception {
        SpringDataExchanger delegate = SpringContextFactory.createSpringContextDelegate(".", "context.xml");
        try {
            delegate.send("hello", new HashMap<String, Object>(), 1000L);
        } finally {
            delegate.close();
        }
    }

    @Test
    public void validateMessageNotReceivedNoToNiFiChannel() throws Exception {
        SpringDataExchanger delegate = SpringContextFactory.createSpringContextDelegate(".", "context.xml");
        SpringResponse<?> fromSpring = delegate.receive(1000L);
        assertNull(fromSpring);
        delegate.close();
    }

    @Test
    public void validateOneWaySent() throws Exception {
        SpringDataExchanger delegate = SpringContextFactory.createSpringContextDelegate(".", "toSpringOnly.xml");
        boolean sent = delegate.send("hello", new HashMap<String, Object>(), 1000L);
        assertTrue(sent);
        SpringResponse<?> fromSpring = delegate.receive(1000L);
        assertNull(fromSpring);
        delegate.close();
    }

    @Test
    public void validateOneWayReceive() throws Exception {
        SpringDataExchanger delegate = SpringContextFactory.createSpringContextDelegate(".", "fromSpringOnly.xml");
        try {
            delegate.send("hello", new HashMap<String, Object>(), 1000L);
        } catch (IllegalStateException e) {
            // ignore since its expected
        }
        SpringResponse<?> fromSpring = delegate.receive(1000L);
        assertNotNull(fromSpring);
        delegate.close();
    }

    @Test
    public void validateRequestReply() throws Exception {
        SpringDataExchanger delegate = SpringContextFactory.createSpringContextDelegate(".", "requestReply.xml");
        boolean sent = delegate.send("hello", new HashMap<String, Object>(), 1000L);
        assertTrue(sent);
        SpringResponse<?> fromSpring = delegate.receive(1000L);
        assertNotNull(fromSpring);
        assertEquals("hello-hello", fromSpring.getPayload());
        assertEquals("foo", fromSpring.getHeaders().get("foo"));
        delegate.close();
    }

    @Test
    public void validateMultipleSendsWithAggregatedReply() throws Exception {
        SpringDataExchanger delegate = SpringContextFactory.createSpringContextDelegate(".", "aggregated.xml");
        // 1
        boolean sent = delegate.send("hello", new HashMap<String, Object>(), 1000L);
        assertTrue(sent);
        SpringResponse<?> fromSpring = delegate.receive(100L);
        assertNull(fromSpring);
        // 2
        sent = delegate.send("hello", new HashMap<String, Object>(), 1000L);
        assertTrue(sent);
        fromSpring = delegate.receive(100L);
        assertNull(fromSpring);
        // 3
        sent = delegate.send("hello", new HashMap<String, Object>(), 1000L);
        assertTrue(sent);
        fromSpring = delegate.receive(100L);
        assertNull(fromSpring);
        // 4
        sent = delegate.send("hello", new HashMap<String, Object>(), 1000L);
        assertTrue(sent);
        fromSpring = delegate.receive(100L);
        assertNotNull(fromSpring);
        assertEquals("4", fromSpring.getPayload());
        delegate.close();
    }
}
