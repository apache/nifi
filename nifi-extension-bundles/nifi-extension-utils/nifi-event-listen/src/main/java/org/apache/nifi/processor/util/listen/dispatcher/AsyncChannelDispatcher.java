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
package org.apache.nifi.processor.util.listen.dispatcher;

import java.nio.channels.SelectionKey;

/**
 * A ChannelDispatcher that handles channels asynchronously.
 */
public interface AsyncChannelDispatcher extends ChannelDispatcher {

    /**
     * Informs the dispatcher that the connection for the given key is complete.
     *
     * @param key a key that was previously selected
     */
    void completeConnection(SelectionKey key);

    /**
     * Informs the dispatcher that the connection for the given key can be added back for selection.
     *
     * @param key a key that was previously selected
     */
    void addBackForSelection(SelectionKey key);

}
