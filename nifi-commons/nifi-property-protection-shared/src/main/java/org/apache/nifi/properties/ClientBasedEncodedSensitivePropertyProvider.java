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
package org.apache.nifi.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Client-Based extension of Encoded Sensitive Property Provider
 *
 * @param <T> Client Type
 */
public abstract class ClientBasedEncodedSensitivePropertyProvider<T> extends EncodedSensitivePropertyProvider {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final T client;

    private final Properties properties;

    public ClientBasedEncodedSensitivePropertyProvider(final T client,
                                                       final Properties properties) {
        this.client = client;
        this.properties = properties;
        validate(client);
    }

    /**
     * Is Provider supported based on client status
     *
     * @return Provider supported status
     */
    @Override
    public boolean isSupported() {
        return client != null;
    }

    /**
     * Clean up resources should be overridden when client requires shutdown
     */
    @Override
    public void cleanUp() {
        logger.debug("Cleanup Started");
    }

    /**
     * Get Client Properties
     *
     * @return Client Properties
     */
    protected Properties getProperties() {
        return properties;
    }

    /**
     * Get Client
     *
     * @return Client can be null when not configured
     */
    protected T getClient() {
        if (client == null) {
            throw new IllegalStateException("Client not configured");
        }
        return client;
    }

    /**
     * Validate Provider and Client configuration
     *
     * @param configuredClient Configured Client
     */
    protected void validate(final T configuredClient) {
        if (configuredClient == null) {
            logger.debug("Client not configured");
        }
    }
}
