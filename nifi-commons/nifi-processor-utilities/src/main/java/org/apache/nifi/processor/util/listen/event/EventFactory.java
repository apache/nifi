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

import java.util.Map;

/**
 * Factory to create instances of a given type of Event.
 */
public interface EventFactory<E extends Event> {

    /**
     * The key in the metadata map for the sender.
     */
    String SENDER_KEY = "sender";

    /**
     * Creates an event for the given data and metadata.
     *
     * @param data raw data from a channel
     * @param metadata additional metadata
     * @param responder a responder for the event with the channel populated
     *
     * @return an instance of the given type
     */
    E create(final byte[] data, final Map<String, String> metadata, final ChannelResponder responder);

}
