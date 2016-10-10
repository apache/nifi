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

import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;

import java.nio.channels.SocketChannel;

/**
 * A Lumberjack event which adds the transaction number and command to the StandardEvent.
 */
public class LumberjackEvent extends StandardEvent<SocketChannel> {

    private final long seqNumber;
    private final String fields;

    public LumberjackEvent(final String sender, final byte[] data, final ChannelResponder<SocketChannel> responder, final long seqNumber, String fields) {
        super(sender, data, responder);
        this.seqNumber = seqNumber;
        this.fields = fields;
    }

    public long getSeqNumber() {
        return seqNumber;
    }

    public String getFields() {
        return fields;
    }

}
