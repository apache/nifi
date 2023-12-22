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
package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class QueryCassandraIT {
    @Container
    private static final CassandraContainer CASSANDRA_CONTAINER = new CassandraContainer(DockerImageName.parse("cassandra:4.1"));

    private static TestRunner putCassandraTestRunner;
    private static TestRunner queryCassandraTestRunner;
    private static MockRecordParser recordReader;

    private static Cluster cluster;
    private static Session session;

    private static final int LOAD_FLOW_FILE_SIZE = 100;
    private static final int LOAD_FLOW_FILE_BATCH_SIZE = 10;

    private static final String KEYSPACE = "sample_keyspace";
    private static final String TABLE = "sample_table";

    @BeforeAll
    public static void setup() throws InitializationException {
        recordReader = new MockRecordParser();
        putCassandraTestRunner = TestRunners.newTestRunner(PutCassandraRecord.class);
        queryCassandraTestRunner = TestRunners.newTestRunner(QueryCassandra.class);

        InetSocketAddress contactPoint = CASSANDRA_CONTAINER.getContactPoint();
        putCassandraTestRunner.setProperty(PutCassandraRecord.RECORD_READER_FACTORY, "reader");
        putCassandraTestRunner.setProperty(PutCassandraRecord.CONTACT_POINTS, contactPoint.getHostString() + ":" + contactPoint.getPort());
        putCassandraTestRunner.setProperty(PutCassandraRecord.KEYSPACE, KEYSPACE);
        putCassandraTestRunner.setProperty(PutCassandraRecord.TABLE, TABLE);
        putCassandraTestRunner.setProperty(PutCassandraRecord.CONSISTENCY_LEVEL, "SERIAL");
        putCassandraTestRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, "LOGGED");
        putCassandraTestRunner.addControllerService("reader", recordReader);
        putCassandraTestRunner.enableControllerService(recordReader);

        queryCassandraTestRunner.setProperty(QueryCassandra.CONTACT_POINTS, contactPoint.getHostName() + ":" + contactPoint.getPort());
        queryCassandraTestRunner.setProperty(QueryCassandra.FETCH_SIZE, "10");
        queryCassandraTestRunner.setProperty(QueryCassandra.OUTPUT_BATCH_SIZE, "10");
        queryCassandraTestRunner.setProperty(QueryCassandra.KEYSPACE, KEYSPACE);
        queryCassandraTestRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "select * from " + TABLE + ";");

        cluster = Cluster.builder().addContactPoint(contactPoint.getHostName())
                .withPort(contactPoint.getPort()).build();
        session = cluster.connect();

        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};";
        String createTable = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + "(id int PRIMARY KEY, uuid text, age int);";

        session.execute(createKeyspace);
        session.execute(createTable);
        loadData();
    }

    private static void loadData() {
        recordReader.addSchemaField("id", RecordFieldType.INT);
        recordReader.addSchemaField("uuid", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        int recordCount = 0;

        for (int i = 0; i<LOAD_FLOW_FILE_SIZE; i++) {
            for (int j = 0; j<LOAD_FLOW_FILE_BATCH_SIZE; j++) {
                recordCount++;
                recordReader.addRecord(recordCount, UUID.randomUUID().toString(),
                        ThreadLocalRandom.current().nextInt(0, 101));
            }
            putCassandraTestRunner.enqueue("");
            putCassandraTestRunner.run();
        }
        putCassandraTestRunner.assertAllFlowFilesTransferred(PutCassandraRecord.REL_SUCCESS, LOAD_FLOW_FILE_SIZE);
        assertEquals(LOAD_FLOW_FILE_SIZE*LOAD_FLOW_FILE_BATCH_SIZE, getRecordsCount());
    }

    @Test
    public void testSimpleQuery() {
        queryCassandraTestRunner.enqueue("");
        queryCassandraTestRunner.run();
        Assertions.assertEquals(1, queryCassandraTestRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).size());
        queryCassandraTestRunner.clearTransferState();
    }

    @Test
    public void testWithoutBatchSize() {
        queryCassandraTestRunner.removeProperty(QueryCassandra.OUTPUT_BATCH_SIZE);
        queryCassandraTestRunner.enqueue("");
        queryCassandraTestRunner.run();
        Assertions.assertEquals(1, queryCassandraTestRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).size());
        queryCassandraTestRunner.clearTransferState();
    }

    @Test
    public void testWithMaxRowsPerFlowFile() {
        queryCassandraTestRunner.setProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE, "10");
        queryCassandraTestRunner.enqueue("");
        queryCassandraTestRunner.run();
        Assertions.assertEquals(100, queryCassandraTestRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).size());
        queryCassandraTestRunner.clearTransferState();
    }

    @Test
    public void testWithDefaults() {
        queryCassandraTestRunner.removeProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE);
        queryCassandraTestRunner.removeProperty(QueryCassandra.OUTPUT_BATCH_SIZE);
        queryCassandraTestRunner.removeProperty(QueryCassandra.FETCH_SIZE);
        queryCassandraTestRunner.enqueue("");
        queryCassandraTestRunner.run();
        Assertions.assertEquals(1, queryCassandraTestRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).size());
        queryCassandraTestRunner.clearTransferState();
    }

    private static int getRecordsCount() {
        Select selectQuery = QueryBuilder.select().all().from(KEYSPACE, TABLE);
        ResultSet result = session.execute(selectQuery);

        List<Integer> resultsList = result.all()
                .stream()
                .map(r -> r.getInt(0))
                .collect(Collectors.toList());

        return resultsList.size();
    }

    private void dropRecords() {
        Truncate query = QueryBuilder.truncate(KEYSPACE, TABLE);
        session.execute(query);
    }

    @AfterAll
    public static void shutdown() {
        String dropKeyspace = "DROP KEYSPACE " + KEYSPACE;
        String dropTable = "DROP TABLE IF EXISTS " + KEYSPACE + "." + TABLE;

        session.execute(dropTable);
        session.execute(dropKeyspace);

        session.close();
        cluster.close();
    }
}
