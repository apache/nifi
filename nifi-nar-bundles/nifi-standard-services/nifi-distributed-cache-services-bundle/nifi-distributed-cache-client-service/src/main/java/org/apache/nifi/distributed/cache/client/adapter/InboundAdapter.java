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
package org.apache.nifi.distributed.cache.client.adapter;

import java.io.IOException;

/**
 * Interface to read service responses from a {@link io.netty.channel.Channel}.  Implementing classes will describe
 * the response content expected for a given service request.
 */
public interface InboundAdapter {

    /**
     * Check for receipt of all response content expected from server.
     *
     * @return true iff the server response has been completely received
     */
    boolean isComplete();

    /**
     * Add {@link io.netty.channel.Channel} content to a queue for later processing
     *
     * @param bytes the remote content to queue
     */
    void queue(final byte[] bytes);

    /**
     * Parse the received content into a form suitable for supplying to the remote method caller.
     *
     * @throws IOException on failure parsing the content
     */
    void dequeue() throws IOException;
}
