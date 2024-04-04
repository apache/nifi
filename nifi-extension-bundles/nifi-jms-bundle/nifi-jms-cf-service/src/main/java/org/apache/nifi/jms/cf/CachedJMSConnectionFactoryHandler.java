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
package org.apache.nifi.jms.cf;

import org.apache.nifi.logging.ComponentLog;

import jakarta.jms.ConnectionFactory;

public abstract class CachedJMSConnectionFactoryHandler implements JMSConnectionFactoryHandlerDefinition {

    private final ComponentLog logger;

    private volatile ConnectionFactory connectionFactory;

    protected CachedJMSConnectionFactoryHandler(ComponentLog logger) {
        this.logger = logger;
    }

    public abstract ConnectionFactory createConnectionFactory();

    @Override
    public synchronized ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            connectionFactory = createConnectionFactory();
        } else {
            logger.debug("Connection Factory has already been initialized. Will return cached instance.");
        }

        return connectionFactory;
    }

    @Override
    public synchronized void resetConnectionFactory(ConnectionFactory cachedFactory) {
        if (cachedFactory == connectionFactory) {
            logger.debug("Resetting connection factory");
            connectionFactory = null;
        }
    }
}
