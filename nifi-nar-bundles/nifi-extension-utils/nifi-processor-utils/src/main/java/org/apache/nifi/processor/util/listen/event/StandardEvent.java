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
package org.apache.nifi.processor.util.listen.event;

import org.apache.nifi.processor.util.listen.response.ChannelResponder;

import java.nio.channels.SelectableChannel;

/**
 * Standard implementation of Event.
 */
public class StandardEvent<C extends SelectableChannel> implements Event<C> {

    private final String sender;
    private final byte[] data;
    private final ChannelResponder<C> responder;

    public StandardEvent(final String sender, final byte[] data, final ChannelResponder<C> responder) {
        this.sender = sender;
        this.data = data;
        this.responder = responder;
    }

    @Override
    public String getSender() {
        return sender;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    public ChannelResponder<C> getResponder() {
        return responder;
    }

}
