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

import org.apache.nifi.processor.util.listen.event.NetworkEventFactory;

import java.util.Map;

/**
 * An EventFactory implementation to create RELPEvents.
 */
public class RELPMessageFactory implements NetworkEventFactory<RELPMessage> {

    @Override
    public RELPMessage create(final byte[] data, final Map<String, String> metadata) {
        final long txnr = Long.valueOf(metadata.get(RELPMetadata.TXNR_KEY));
        final String command = metadata.get(RELPMetadata.COMMAND_KEY);
        final String sender = metadata.get(RELPMetadata.SENDER_KEY);
        return new RELPMessage(sender, data, txnr, command);
    }
}
