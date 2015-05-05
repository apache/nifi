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
package org.apache.nifi.cluster.protocol.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;
import org.apache.nifi.io.socket.multicast.ServiceDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the ServiceLocator interface for locating the socket address of a
 * cluster service. Depending on configuration, the address may be located using
 * service discovery. If using service discovery, then the service methods must
 * be used for starting and stopping discovery.
 *
 * Service discovery may be used in conjunction with a fixed port. In this case,
 * the service discovery will yield the service IP/host while the fixed port
 * will be used for the port.
 *
 * Alternatively, the instance may be configured with exact service location, in
 * which case, no service discovery occurs and the caller will always receive
 * the configured service.
 *
 * @author unattributed
 */
public class ClusterServiceLocator implements ServiceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(ClusterServiceLocator.class);

    private final String serviceName;

    private final ClusterServiceDiscovery serviceDiscovery;

    private final DiscoverableService fixedService;

    private final int fixedServicePort;

    private final AttemptsConfig attemptsConfig = new AttemptsConfig();

    private final AtomicBoolean running = new AtomicBoolean(false);

    public ClusterServiceLocator(final ClusterServiceDiscovery serviceDiscovery) {
        if (serviceDiscovery == null) {
            throw new IllegalArgumentException("ClusterServiceDiscovery may not be null.");
        }
        this.serviceDiscovery = serviceDiscovery;
        this.fixedService = null;
        this.fixedServicePort = 0;
        this.serviceName = serviceDiscovery.getServiceName();
    }

    public ClusterServiceLocator(final ClusterServiceDiscovery serviceDiscovery, final int fixedServicePort) {
        if (serviceDiscovery == null) {
            throw new IllegalArgumentException("ClusterServiceDiscovery may not be null.");
        }
        this.serviceDiscovery = serviceDiscovery;
        this.fixedService = null;
        this.fixedServicePort = fixedServicePort;
        this.serviceName = serviceDiscovery.getServiceName();
    }

    public ClusterServiceLocator(final DiscoverableService fixedService) {
        if (fixedService == null) {
            throw new IllegalArgumentException("Service may not be null.");
        }
        this.serviceDiscovery = null;
        this.fixedService = fixedService;
        this.fixedServicePort = 0;
        this.serviceName = fixedService.getServiceName();
    }

    @Override
    public DiscoverableService getService() {

        final int numAttemptsValue;
        final int secondsBetweenAttempts;
        synchronized (this) {
            numAttemptsValue = attemptsConfig.numAttempts;
            secondsBetweenAttempts = attemptsConfig.getTimeBetweenAttempts();
        }

        // try for a configured amount of attempts to retrieve the service address
        for (int i = 0; i < numAttemptsValue; i++) {

            if (fixedService != null) {
                return fixedService;
            } else if (serviceDiscovery != null) {

                final DiscoverableService discoveredService = serviceDiscovery.getService();

                // if we received an address
                if (discoveredService != null) {
                    // if we were configured with a fixed port, then use the discovered host and fixed port; otherwise use the discovered address
                    if (fixedServicePort > 0) {
                        // create service using discovered service name and address with fixed service port
                        final InetSocketAddress addr = InetSocketAddress.createUnresolved(discoveredService.getServiceAddress().getHostName(), fixedServicePort);
                        final DiscoverableService result = new DiscoverableServiceImpl(discoveredService.getServiceName(), addr);
                        return result;
                    } else {
                        return discoveredService;
                    }
                }
            }

            // could not obtain service address, so sleep a bit
            try {
                logger.debug(String.format("Locating Cluster Service '%s' Attempt: %d of %d failed.  Trying again in %d seconds.",
                        serviceName, (i + 1), numAttemptsValue, secondsBetweenAttempts));
                Thread.sleep(secondsBetweenAttempts * 1000);
            } catch (final InterruptedException ie) {
                break;
            }

        }

        return null;
    }

    public boolean isRunning() {
        if (serviceDiscovery != null) {
            return serviceDiscovery.isRunning();
        } else {
            return running.get();
        }
    }

    public void start() throws IOException {

        if (isRunning()) {
            throw new IllegalStateException("Instance is already started.");
        }

        if (serviceDiscovery != null) {
            serviceDiscovery.start();
        }
        running.set(true);
    }

    public void stop() throws IOException {

        if (isRunning() == false) {
            throw new IllegalStateException("Instance is already stopped.");
        }

        if (serviceDiscovery != null) {
            serviceDiscovery.stop();
        }
        running.set(false);
    }

    public synchronized void setAttemptsConfig(final AttemptsConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Attempts configuration may not be null.");
        }
        this.attemptsConfig.numAttempts = config.numAttempts;
        this.attemptsConfig.timeBetweenAttempts = config.timeBetweenAttempts;
        this.attemptsConfig.timeBetweenAttempsUnit = config.timeBetweenAttempsUnit;
    }

    public synchronized AttemptsConfig getAttemptsConfig() {
        final AttemptsConfig config = new AttemptsConfig();
        config.numAttempts = this.attemptsConfig.numAttempts;
        config.timeBetweenAttempts = this.attemptsConfig.timeBetweenAttempts;
        config.timeBetweenAttempsUnit = this.attemptsConfig.timeBetweenAttempsUnit;
        return config;
    }

    public static class AttemptsConfig {

        private int numAttempts = 1;

        private int timeBetweenAttempts = 1;

        private TimeUnit timeBetweenAttempsUnit = TimeUnit.SECONDS;

        public int getNumAttempts() {
            return numAttempts;
        }

        public void setNumAttempts(int numAttempts) {
            if (numAttempts <= 0) {
                throw new IllegalArgumentException("Number of attempts must be positive: " + numAttempts);
            }
            this.numAttempts = numAttempts;
        }

        public TimeUnit getTimeBetweenAttemptsUnit() {
            return timeBetweenAttempsUnit;
        }

        public void setTimeBetweenAttempsUnit(TimeUnit timeBetweenAttempsUnit) {
            if (timeBetweenAttempts <= 0) {
                throw new IllegalArgumentException("Time between attempts must be positive: " + numAttempts);
            }
            this.timeBetweenAttempsUnit = timeBetweenAttempsUnit;
        }

        public int getTimeBetweenAttempts() {
            return timeBetweenAttempts;
        }

        public void setTimeBetweenAttempts(int timeBetweenAttempts) {
            if (timeBetweenAttempts <= 0) {
                throw new IllegalArgumentException("Time between attempts must be positive: " + numAttempts);
            }
            this.timeBetweenAttempts = timeBetweenAttempts;
        }

    }
}
