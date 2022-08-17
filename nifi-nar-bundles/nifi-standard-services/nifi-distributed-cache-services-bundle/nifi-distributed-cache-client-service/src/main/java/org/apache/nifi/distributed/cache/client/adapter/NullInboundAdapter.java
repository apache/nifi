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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of {@link InboundAdapter} where no service data is expected.  Data is only expected
 * to be received in the context of a client call to the service; any extraneous data received from
 * a {@link io.netty.channel.Channel} may be safely dropped.
 */
public class NullInboundAdapter implements InboundAdapter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public void queue(final byte[] bytes) {
        logger.debug("Received and dropped {} extraneous bytes.", bytes.length);
    }

    @Override
    public void dequeue() throws IOException {
    }
}
