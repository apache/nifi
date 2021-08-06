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
package org.apache.nifi.distributed.cache.client;

import org.apache.nifi.distributed.cache.client.adapter.InboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.OutboundAdapter;

import java.io.IOException;

/**
 * Encapsulate operations which may be performed using a {@link DistributedSetCacheClientService} or a
 * {@link DistributedMapCacheClientService}.
 */
public interface DistributedCacheClient {

    /**
     * Call a service method.
     *
     * @param outboundAdapter the object used to assemble the service request byte stream
     * @param inboundAdapter  the object used to interpret the service response byte stream
     * @throws IOException on serialization failure; on communication failure
     */
    void invoke(OutboundAdapter outboundAdapter, InboundAdapter inboundAdapter) throws IOException;
}
