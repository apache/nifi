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
package org.apache.nifi.redis.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.RedisType;
import org.apache.nifi.redis.util.RedisUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import java.util.Collection;
import java.util.List;

@Tags({"redis", "cache"})
@CapabilityDescription("A service that provides connections to Redis.")
public class RedisConnectionPoolService extends AbstractControllerService implements RedisConnectionPool {

    private volatile PropertyContext context;
    private volatile RedisType redisType;
    private volatile JedisConnectionFactory connectionFactory;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return RedisUtils.REDIS_CONNECTION_PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return RedisUtils.validate(validationContext);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.context = context;

        final String redisMode = context.getProperty(RedisUtils.REDIS_MODE).getValue();
        this.redisType = RedisType.fromDisplayName(redisMode);
    }

    @OnDisabled
    public void onDisabled() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
            connectionFactory = null;
            redisType = null;
            context = null;
        }
    }

    @Override
    public RedisType getRedisType() {
        return redisType;
    }

    @Override
    public RedisConnection getConnection() {
        if (connectionFactory == null) {
            synchronized (this) {
                if (connectionFactory == null) {
                    connectionFactory = RedisUtils.createConnectionFactory(context, getLogger());
                }
            }
        }

        return connectionFactory.getConnection();
    }


}
