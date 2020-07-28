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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

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
