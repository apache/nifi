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
package org.apache.nifi.pulsar.pool;

import java.util.Properties;

public interface ResourcePool<R extends PoolableResource> {

    /**
     * Acquire a resource from the pool. Creating one if necessary
     */
   public R acquire(Properties props) throws InterruptedException;

    /**
     * Evict the resource from the pool, destroying it.
     * Call this method is the resource is known to be in an unusable state.
     */
    public void evict(R resource);

    /**
     * Place the resource back into the pool for future use.
     */
    public void release(R resource);
}
