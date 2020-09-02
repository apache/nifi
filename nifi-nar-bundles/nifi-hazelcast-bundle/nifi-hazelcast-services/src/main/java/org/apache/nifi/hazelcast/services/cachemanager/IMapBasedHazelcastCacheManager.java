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

import com.hazelcast.core.HazelcastInstance;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hazelcast.services.cache.HazelcastCache;
import org.apache.nifi.hazelcast.services.cache.IMapBasedHazelcastCache;
import org.apache.nifi.reporting.InitializationException;

abstract class IMapBasedHazelcastCacheManager extends AbstractControllerService implements HazelcastCacheManager {
    protected static final String ADDRESS_SEPARATOR = ",";

    public static final PropertyDescriptor HAZELCAST_CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("hazelcast-cluster-name")
            .displayName("Hazelcast Cluster Name")
            .description("Name of the Hazelcast instance's cluster.")
            .defaultValue("nifi") // Hazelcast's default is "dev", "nifi" overwrites this.
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private volatile HazelcastInstance instance;

    @Override
    public HazelcastCache getCache(final String name, final long ttlInMillis) {
        return new IMapBasedHazelcastCache(name, ttlInMillis, instance.getMap(name));
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            instance = getInstance(context);
        } catch (final Exception e) {
            getLogger().error("Could not initialize Hazelcast connection. Reason: " + e.getMessage(), e);
            throw new InitializationException(e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        if (instance != null) {
            instance.shutdown();
        }

        instance = null;
    }

    protected abstract HazelcastInstance getInstance(final ConfigurationContext context);
}
