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

import com.datastax.driver.core.Cluster
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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertTrue

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
@Testcontainers
class CassandraDistributedMapCacheIT {
    @Container
    static final CassandraContainer CASSANDRA_CONTAINER = new CassandraContainer(DockerImageName.parse("cassandra:4.1"))
    static TestRunner runner
    static CassandraDistributedMapCache distributedMapCache
    static Session session

    static final String KEYSPACE = "sample_keyspace"

    @BeforeAll
    static void setup() {
        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

            }
        })
        distributedMapCache = new CassandraDistributedMapCache()

        InetSocketAddress contactPoint = CASSANDRA_CONTAINER.getContactPoint()
        String connectionString = String.format("%s:%d", contactPoint.getHostName(), contactPoint.getPort())

        Cluster cluster = Cluster.builder().addContactPoint(contactPoint.getHostName())
                .withPort(contactPoint.getPort()).build();
        session = cluster.connect();

        session.execute("create keyspace nifi_test with replication = { 'replication_factor': 1, 'class': 'SimpleStrategy' }");
        session.execute("create table nifi_test.dmc (id blob, value blob, primary key(id))");

        def cassandraService = new CassandraSessionProvider()
        runner.addControllerService("provider", cassandraService)
        runner.addControllerService("dmc", distributedMapCache)
        runner.setProperty(cassandraService, CassandraSessionProvider.CONTACT_POINTS, connectionString)
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

    @AfterAll
    static void cleanup() {
        session.execute("TRUNCATE nifi_test.dmc")
    }

    Serializer<String> serializer = { str, os ->
        os.write(str.bytes)
    } as Serializer

    Deserializer<String> deserializer = { input ->
        new String(input)
    } as Deserializer

    @Test
    void testContainsKey() {
        assertTrue(distributedMapCache.containsKey("contains-key", serializer))
    }

    @Test
    void testGetAndPutIfAbsent() {
        String result = distributedMapCache.getAndPutIfAbsent('get-and-put-key', 'testing', serializer, serializer, deserializer)
        assertEquals("testvalue", result)
    }

    @Test
    void testRemove() {
        distributedMapCache.remove("delete-key", serializer)
    }

    @Test
    void testGet() {
        String result = distributedMapCache.get("contains-key", serializer, deserializer)
        assertEquals("testvalue", result)
    }

    @Test
    void testPut() {
        distributedMapCache.put("put-key", "sometestdata", serializer, serializer)
        Thread.sleep(1000)
        assertTrue(distributedMapCache.containsKey("put-key", serializer))
    }

    @Test
    void testPutIfAbsent() {
        assertTrue(distributedMapCache.putIfAbsent("put-if-absent-key", "testingthis", serializer, serializer))
        assertFalse(distributedMapCache.putIfAbsent("put-if-absent-key", "testingthis", serializer, serializer))
    }
}
