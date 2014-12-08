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
package org.apache.nifi.io.socket.multicast;

import java.util.Set;

/**
 * Defines the interface for broadcasting a collection of services for client
 * discovery.
 *
 * @author unattributed
 */
public interface ServicesBroadcaster {

    /**
     * @return the delay in milliseconds to wait between successive broadcasts
     */
    int getBroadcastDelayMs();

    /**
     * @return the broadcasted services
     */
    Set<DiscoverableService> getServices();

    /**
     * Adds the given service to the set of broadcasted services.
     *
     * @param service a service
     * @return true if the service was added to the set; false a service with
     * the given service name already exists in the set.
     */
    boolean addService(DiscoverableService service);

    /**
     * Removes the service with the given service name from the set.
     *
     * @param serviceName a service name
     * @return true if the service was removed; false otherwise
     */
    boolean removeService(String serviceName);

}
