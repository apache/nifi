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

/**
 * Factory pattern interface for @PoolableResource objects. Concrete implementations
 * of this interface will be responsible for the creation of @PoolableResource objects
 * based on the Properties passed in.
 *
 * @author david
 *
 * @param <R> The type of @PoolableResource this factory will create
 *
 */
public interface ResourceFactory<R extends PoolableResource> {

    /**
     * Create a @PoolableResource based on the properties passed in.
     * @param props The properties used to configure the @PoolableResource
     * @return @PoolableResource of type R
     * @throws ResourceCreationException if the factory cannot instantiate a @PoolableResource object
     */
    public R create(Properties props) throws ResourceCreationException;
}
