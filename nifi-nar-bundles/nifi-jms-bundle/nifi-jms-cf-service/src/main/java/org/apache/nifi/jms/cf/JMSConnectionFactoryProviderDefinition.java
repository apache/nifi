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

import javax.jms.ConnectionFactory;

import org.apache.nifi.controller.ControllerService;

/**
 * Defines a strategy to create implementations to load and initialize third
 * party implementations of the {@link ConnectionFactory}
 */
public interface JMSConnectionFactoryProviderDefinition extends ControllerService {

    /**
     * Returns an instance of the {@link ConnectionFactory} specific to the
     * target messaging system (i.e.,
     * org.apache.activemq.ActiveMQConnectionFactory).
     *
     * @return instance of {@link ConnectionFactory}
     */
    ConnectionFactory getConnectionFactory();

    /**
     * Resets {@link ConnectionFactory}.
     * Provider should reset {@link ConnectionFactory} only if a copy provided by a client matches
     * current {@link ConnectionFactory}.
     * @param cachedFactory - {@link ConnectionFactory} cached by client.
     */
    void resetConnectionFactory(ConnectionFactory cachedFactory);

}
