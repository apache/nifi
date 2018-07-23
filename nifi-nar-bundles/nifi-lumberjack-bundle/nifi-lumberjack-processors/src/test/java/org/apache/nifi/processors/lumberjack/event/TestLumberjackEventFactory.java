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

package org.apache.nifi.processors.lumberjack.event;

import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestLumberjackEventFactory {

    @Test
    public void testCreateLumberJackEvent() {
        final String sender = "testsender1";
        final byte[] data = "this is a test line".getBytes();
        final long seqNumber = 1;
        final String fields = "{\"file\":\"test\"}";


        final Map<String,String> metadata = new HashMap<>();
        metadata.put(EventFactory.SENDER_KEY, sender);
        metadata.put(LumberjackMetadata.SEQNUMBER_KEY, String.valueOf(seqNumber));
        metadata.put(LumberjackMetadata.FIELDS_KEY, String.valueOf(fields));

        final ChannelResponder responder = new SocketChannelResponder(null);

        final EventFactory<LumberjackEvent> factory = new LumberjackEventFactory();

        final LumberjackEvent event = factory.create(data, metadata, responder);

        Assert.assertEquals(sender, event.getSender());
        Assert.assertEquals(seqNumber, event.getSeqNumber());
        Assert.assertEquals(fields, event.getFields());
        Assert.assertEquals(data, event.getData());
    }
}
