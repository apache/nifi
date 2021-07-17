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
package org.apache.nifi.hazelcast.services.cachemanager;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.hazelcast.services.cache.HazelcastCache;

/**
 * Controller service responsible for providing cache instances and managing connection with the Hazelcast server.
 */
public interface HazelcastCacheManager extends ControllerService {

    /**
     * Returns a cache instance maintaining a Hazelcast connection.
     *
     * @param name Name of the cache instance. Cache instances having the same name are depending on the same Hazelcast storage!
     * @param ttlInMillis The guaranteed lifetime of a cache entry in milliseconds. In case of 0, the entry will exists until it's deletion.
     *
     * @return Cache instance. Depending on the implementation it is not guaranteed that it will be a new instance.
     */
    HazelcastCache getCache(String name, long ttlInMillis);
}
