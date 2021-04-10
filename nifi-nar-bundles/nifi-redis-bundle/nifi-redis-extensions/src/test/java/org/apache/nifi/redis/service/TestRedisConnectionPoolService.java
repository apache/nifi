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
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.StandardProcessorTestRunner;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;

public class TestRedisConnectionPoolService {

    public static final String SSL_CONTEXT_IDENTIFIER = "ssl-context-service";
    private TestRunner testRunner;
    private FakeRedisProcessor proc;
    private RedisConnectionPool redisService;

    private static SSLContext sslContext;

    @BeforeClass
    public static void classSetup() throws IOException, GeneralSecurityException {
        sslContext = SSLContext.getDefault();
    }

    @Before
    public void setup() throws InitializationException {
        proc = new FakeRedisProcessor();
        testRunner = TestRunners.newTestRunner(proc);

        redisService = new RedisConnectionPoolService();
        testRunner.addControllerService("redis-service", redisService);
    }

    private void enableSslContextService() throws InitializationException {
        final RestrictedSSLContextService sslContextService = Mockito.mock(RestrictedSSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_IDENTIFIER);
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);
        testRunner.addControllerService(SSL_CONTEXT_IDENTIFIER, sslContextService);
        testRunner.enableControllerService(sslContextService);
        testRunner.setProperty(redisService, RedisUtils.SSL_CONTEXT_SERVICE, SSL_CONTEXT_IDENTIFIER);
    }

    @Test
    public void testSSLContextService() throws InitializationException {
        this.setDefaultRedisProperties();

        this.enableSslContextService();

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
        SSLContext providedSslContext = null;
        if (configContext.getProperty(RedisUtils.SSL_CONTEXT_SERVICE).isSet()) {
            final SSLContextService sslContextService = configContext.getProperty(RedisUtils.SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            providedSslContext = sslContextService.createContext();
        }
        JedisConnectionFactory connectionFactory = RedisUtils.createConnectionFactory(configContext, testRunner.getLogger(), providedSslContext);
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
