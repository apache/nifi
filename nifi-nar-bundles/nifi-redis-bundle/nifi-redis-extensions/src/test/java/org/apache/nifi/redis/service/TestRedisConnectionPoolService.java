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

import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.StandardProcessorTestRunner;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

public class TestRedisConnectionPoolService {

    private TestRunner testRunner;
    private FakeRedisProcessor proc;
    private RedisConnectionPool redisService;

    @Before
    public void setup() throws InitializationException {
        proc = new FakeRedisProcessor();
        testRunner = TestRunners.newTestRunner(proc);

        redisService = new RedisConnectionPoolService();
        testRunner.addControllerService("redis-service", redisService);
    }

    @Test
    public void testSSLContextService() throws InitializationException {
        StandardRestrictedSSLContextService sslContextService = new StandardRestrictedSSLContextService();
        testRunner.addControllerService("ssl-context-service", sslContextService);
        testRunner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
        testRunner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "passwordpassword");
        testRunner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        testRunner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        testRunner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "passwordpassword");
        testRunner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        testRunner.setProperty(sslContextService, StandardSSLContextService.SSL_ALGORITHM, TlsConfiguration.TLS_1_2_PROTOCOL);
        testRunner.enableControllerService(sslContextService);

        testRunner.setProperty(redisService, RedisUtils.SSL_CONTEXT_SERVICE, "ssl-context-service");

        this.setDefaultRedisProperties();

        testRunner.assertValid(redisService);
        testRunner.enableControllerService(redisService);

        JedisConnectionFactory connectionFactory = getJedisConnectionFactory(); // Uses config from test runner

        // Verify that the client configuration will be using an SSL socket factory and SSL parameters
        Assert.assertTrue(connectionFactory.getClientConfiguration().getSslSocketFactory().isPresent());
        Assert.assertTrue(connectionFactory.getClientConfiguration().getSslParameters().isPresent());

        // Now remove the SSL context service
        testRunner.disableControllerService(redisService);
        testRunner.removeProperty(redisService, RedisUtils.SSL_CONTEXT_SERVICE);
        testRunner.enableControllerService(redisService);
        connectionFactory = getJedisConnectionFactory();

        // Now the client configuration will not use SSL
        Assert.assertFalse(connectionFactory.getClientConfiguration().getSslSocketFactory().isPresent());
        Assert.assertFalse(connectionFactory.getClientConfiguration().getSslParameters().isPresent());
    }

    private void setDefaultRedisProperties() {
        testRunner.setProperty(redisService, RedisUtils.REDIS_MODE, "Standalone");
        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379");
        testRunner.setProperty(redisService, RedisUtils.DATABASE, "0");
        testRunner.setProperty(redisService, RedisUtils.COMMUNICATION_TIMEOUT, "60s");
        testRunner.setProperty(redisService, RedisUtils.CLUSTER_MAX_REDIRECTS, "5");
        testRunner.setProperty(redisService, RedisUtils.POOL_MAX_TOTAL, "1");
        testRunner.setProperty(redisService, RedisUtils.POOL_MAX_IDLE, "1");
        testRunner.setProperty(redisService, RedisUtils.POOL_MIN_IDLE, "1");
        testRunner.setProperty(redisService, RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED, "true");
        testRunner.setProperty(redisService, RedisUtils.POOL_MAX_WAIT_TIME, "1s");
        testRunner.setProperty(redisService, RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME, "1s");
        testRunner.setProperty(redisService, RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS, "1s");
        testRunner.setProperty(redisService, RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN, "1");
        testRunner.setProperty(redisService, RedisUtils.POOL_TEST_ON_CREATE, "false");
        testRunner.setProperty(redisService, RedisUtils.POOL_TEST_ON_BORROW, "false");
        testRunner.setProperty(redisService, RedisUtils.POOL_TEST_ON_RETURN, "false");
        testRunner.setProperty(redisService, RedisUtils.POOL_TEST_WHILE_IDLE, "false");
    }

    private JedisConnectionFactory getJedisConnectionFactory() {
        MockProcessContext processContext = ((StandardProcessorTestRunner) testRunner).getProcessContext();
        MockConfigurationContext configContext = new MockConfigurationContext(processContext.getControllerServices()
                .get(redisService.getIdentifier()).getProperties(), processContext);
        JedisConnectionFactory connectionFactory = RedisUtils.createConnectionFactory(configContext, testRunner.getLogger());
        return connectionFactory;
    }

    @Test
    public void testValidateConnectionString() {
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, " ");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "${redis.connection}");
        testRunner.assertNotValid(redisService);

        testRunner.setVariable("redis.connection", "localhost:6379");
        testRunner.assertValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:a");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379");
        testRunner.assertValid(redisService);

        // standalone can only have one host:port pair
        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379,localhost:6378");
        testRunner.assertNotValid(redisService);

        // cluster can have multiple host:port pairs
        testRunner.setProperty(redisService, RedisUtils.REDIS_MODE, RedisUtils.REDIS_MODE_CLUSTER.getValue());
        testRunner.assertValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379,localhost");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "local:host:6379,localhost:6378");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:a,localhost:b");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost  :6379,  localhost  :6378,    localhost:6377");
        testRunner.assertValid(redisService);
    }

    @Test
    public void testValidateSentinelMasterRequiredInSentinelMode() {
        testRunner.setProperty(redisService, RedisUtils.REDIS_MODE, RedisUtils.REDIS_MODE_SENTINEL.getValue());
        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379,localhost:6378");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.SENTINEL_MASTER, "mymaster");
        testRunner.assertValid(redisService);
    }

}
