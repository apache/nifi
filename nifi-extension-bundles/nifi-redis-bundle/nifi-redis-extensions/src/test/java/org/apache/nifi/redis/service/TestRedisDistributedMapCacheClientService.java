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
import org.apache.nifi.redis.RedisType;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRedisDistributedMapCacheClientService {

    @ParameterizedTest
    @EnumSource(RedisType.class)
    public void testAcceptsConnectionTopologies(final RedisType redisType) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new FakeRedisProcessor());
        final RedisConnectionPool pool = mock(RedisConnectionPool.class);
        when(pool.getIdentifier()).thenReturn("pool");
        when(pool.getRedisType()).thenReturn(redisType);
        runner.addControllerService("pool", pool);
        runner.enableControllerService(pool);
        final RedisDistributedMapCacheClientService cache = new RedisDistributedMapCacheClientService();
        runner.addControllerService("cache", cache);
        runner.setProperty(cache, RedisUtils.REDIS_CONNECTION_POOL, "pool");
        runner.assertValid(cache);
    }
}
