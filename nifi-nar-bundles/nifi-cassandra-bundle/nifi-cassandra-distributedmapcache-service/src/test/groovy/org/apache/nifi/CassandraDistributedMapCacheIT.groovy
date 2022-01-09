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
package org.apache.nifi

import com.datastax.driver.core.Session
import org.apache.nifi.controller.cassandra.CassandraDistributedMapCache
import org.apache.nifi.distributed.cache.client.Deserializer
import org.apache.nifi.distributed.cache.client.Serializer
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.service.CassandraSessionProvider
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
/**
 * Setup instructions:
 *
 * docker run -p 7000:7000 -p 9042:9042 --name cassandra --restart always -d cassandra:3
 *
 * docker exec -it cassandra cqlsh
 *
 * Keyspace CQL: create keyspace nifi_test with replication = { 'replication_factor': 1, 'class': 'SimpleStrategy' } ;
 *
 * Table SQL: create table dmc (id blob, value blob, primary key(id));
 */
class CassandraDistributedMapCacheIT {
    static TestRunner runner
    static CassandraDistributedMapCache distributedMapCache
    static Session session

    @BeforeClass
    static void setup() {
        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

            }
        })
        distributedMapCache = new CassandraDistributedMapCache()

        def cassandraService = new CassandraSessionProvider()
        runner.addControllerService("provider", cassandraService)
        runner.addControllerService("dmc", distributedMapCache)
        runner.setProperty(cassandraService, CassandraSessionProvider.CONTACT_POINTS, "localhost:9042")
        runner.setProperty(cassandraService, CassandraSessionProvider.KEYSPACE, "nifi_test")
        runner.setProperty(distributedMapCache, CassandraDistributedMapCache.SESSION_PROVIDER, "provider")
        runner.setProperty(distributedMapCache, CassandraDistributedMapCache.TABLE_NAME, "dmc")
        runner.setProperty(distributedMapCache, CassandraDistributedMapCache.KEY_FIELD_NAME, "id")
        runner.setProperty(distributedMapCache, CassandraDistributedMapCache.VALUE_FIELD_NAME, "value")
        runner.setProperty(distributedMapCache, CassandraDistributedMapCache.TTL, "5 sec")
        runner.enableControllerService(cassandraService)
        runner.enableControllerService(distributedMapCache)
        runner.assertValid()

        session = cassandraService.getCassandraSession();
        session.execute("""
            INSERT INTO dmc (id, value) VALUES(textAsBlob('contains-key'), textAsBlob('testvalue'))
        """)
        session.execute("""
            INSERT INTO dmc (id, value) VALUES(textAsBlob('delete-key'), textAsBlob('testvalue'))
        """)
        session.execute("""
            INSERT INTO dmc (id, value) VALUES(textAsBlob('get-and-put-key'), textAsBlob('testvalue'))
        """)
    }

    @AfterClass
    static void cleanup() {
        session.execute("TRUNCATE dmc")
    }

    Serializer<String> serializer = { str, os ->
        os.write(str.bytes)
    } as Serializer

    Deserializer<String> deserializer = { input ->
        new String(input)
    } as Deserializer

    @Test
    void testContainsKey() {
        def contains = distributedMapCache.containsKey("contains-key", serializer)
        assert contains
    }

    @Test
    void testGetAndPutIfAbsent() {
        def result = distributedMapCache.getAndPutIfAbsent('get-and-put-key', 'testing', serializer, serializer, deserializer)
        assert result == 'testvalue'
    }

    @Test
    void testRemove() {
        distributedMapCache.remove("delete-key", serializer)
    }

    @Test
    void testGet() {
        def result = distributedMapCache.get("contains-key", serializer, deserializer)
        assert result == "testvalue"
    }

    @Test
    void testPut() {
        distributedMapCache.put("put-key", "sometestdata", serializer, serializer)
        Thread.sleep(1000)
        assert distributedMapCache.containsKey("put-key", serializer)
    }

    @Test
    void testPutIfAbsent() {
        assert distributedMapCache.putIfAbsent("put-if-absent-key", "testingthis", serializer, serializer)
        assert !distributedMapCache.putIfAbsent("put-if-absent-key", "testingthis", serializer, serializer)
    }
}
