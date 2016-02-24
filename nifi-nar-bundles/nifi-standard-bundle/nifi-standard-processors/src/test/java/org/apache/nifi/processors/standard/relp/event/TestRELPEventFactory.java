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
package org.apache.nifi.processors.standard.relp.event;

import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TestRELPEventFactory {

    @Test
    public void testCreateRELPEvent() {
        final byte[] data = "this is an event".getBytes(StandardCharsets.UTF_8);

        final String sender = "sender1";
        final long txnr = 1;
        final String command = "syslog";

        final Map<String,String> metadata = new HashMap<>();
        metadata.put(EventFactory.SENDER_KEY, sender);
        metadata.put(RELPMetadata.TXNR_KEY, String.valueOf(txnr));
        metadata.put(RELPMetadata.COMMAND_KEY, command);

        final ChannelResponder responder = new SocketChannelResponder(null);

        final EventFactory<RELPEvent> factory = new RELPEventFactory();

        final RELPEvent event = factory.create(data, metadata, responder);
        Assert.assertEquals(data, event.getData());
        Assert.assertEquals(sender, event.getSender());
        Assert.assertEquals(txnr, event.getTxnr());
        Assert.assertEquals(command, event.getCommand());
        Assert.assertEquals(responder, event.getResponder());
    }
}
