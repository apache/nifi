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
package org.apache.nifi.processor.util.listen.response;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.List;

/**
 * A responder for a given channel.
 *
 * @param <C> The type of SelectableChannel where the response will be written.
 */
public interface ChannelResponder<C extends SelectableChannel> {

    /**
     * @return a SelectableChannel to write the response to
     */
    C getChannel();

    /**
     * @return a list of responses to write to the channel
     */
    List<ChannelResponse> getResponses();

    /**
     * @param response adds the given response to the list of responses
     */
    void addResponse(ChannelResponse response);

    /**
     * Writes the responses to the underlying channel.
     */
    void respond() throws IOException;

}
