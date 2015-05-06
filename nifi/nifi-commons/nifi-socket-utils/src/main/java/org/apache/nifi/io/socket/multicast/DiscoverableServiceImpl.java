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

import java.net.InetSocketAddress;
import org.apache.commons.lang3.StringUtils;

/**
 * A basic implementation of the DiscoverableService interface. To services are
 * considered equal if they have the same case-sensitive service name.
 *
 */
public class DiscoverableServiceImpl implements DiscoverableService {

    private final String serviceName;

    private final InetSocketAddress serviceAddress;

    public DiscoverableServiceImpl(final String serviceName, final InetSocketAddress serviceAddress) {
        if (StringUtils.isBlank(serviceName)) {
            throw new IllegalArgumentException("Service name may not be null or empty.");
        } else if (serviceAddress == null) {
            throw new IllegalArgumentException("Service address may not be null.");
        }
        this.serviceName = serviceName;
        this.serviceAddress = serviceAddress;
    }

    @Override
    public InetSocketAddress getServiceAddress() {
        return serviceAddress;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public String toString() {
        return String.format("[Discoverable Service: %s available at %s:%d]", serviceName, serviceAddress.getHostName(), serviceAddress.getPort());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DiscoverableService)) {
            return false;
        }
        final DiscoverableService other = (DiscoverableService) obj;
        return !((this.serviceName == null) ? (other.getServiceName() != null) : !this.serviceName.equals(other.getServiceName()));
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + (this.serviceName != null ? this.serviceName.hashCode() : 0);
        return hash;
    }

}
