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

/**
 * Service interface for any object that can be pooled for re-use., which
 * defines methods for closing the object, effectively marking it no longer
 * usable.
 *
 * @author david
 *
 */
public interface PoolableResource {

    /**
     * Closes the object, marking it as no longer usable.
     * Typically this is called if interactions with the
     * object have resulted in some sort of communication error.
     */
    public void close();

    /**
     * Check to see if the object is usable.
     *
     * @return true if the close method on this object has been called.
     */
    public boolean isClosed();

}
