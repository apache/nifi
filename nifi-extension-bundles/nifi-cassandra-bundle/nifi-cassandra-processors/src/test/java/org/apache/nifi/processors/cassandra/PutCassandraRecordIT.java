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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PutCassandraRecordIT {

    private static final CassandraContainer CASSANDRA_CONTAINER;

    static {
        CASSANDRA_CONTAINER = new CassandraContainer(DockerImageName.parse("cassandra:5.0.6"));
        CASSANDRA_CONTAINER.start();
    }

    private static TestRunner testRunner;
    private static MockRecordParser recordReader;
    private static CqlSession session;

    private static final String KEYSPACE = "sample_keyspace";
    private static final String TABLE = "sample_table";

    @BeforeAll
    public static void setup() throws InitializationException {
        recordReader = new MockRecordParser();
        testRunner = TestRunners.newTestRunner(PutCassandraRecord.class);
        testRunner.setValidateExpressionUsage(false);

        InetSocketAddress contactPoint = CASSANDRA_CONTAINER.getContactPoint();

        testRunner.setProperty(PutCassandraRecord.RECORD_READER_FACTORY, "reader");
        testRunner.setProperty(
                PutCassandraRecord.CONTACT_POINTS,
                contactPoint.getHostString() + ":" + contactPoint.getPort());
        testRunner.setProperty(PutCassandraRecord.KEYSPACE, KEYSPACE);
        testRunner.setProperty(PutCassandraRecord.TABLE, TABLE);
        testRunner.setProperty(PutCassandraRecord.CONSISTENCY_LEVEL, "SERIAL");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, "LOGGED");

        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);

        session = CqlSession.builder()
                .addContactPoint(contactPoint)
                .withLocalDatacenter("datacenter1")
                .build();

        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':1};");

        session.execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE +
                        " (id int PRIMARY KEY, name text, age int);");
    }

    @Test
    public void testSimplePut() {
        recordReader.addSchemaField("id", RecordFieldType.INT);
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);

        recordReader.addRecord(1, "Ram", 42);
        recordReader.addRecord(2, "Jeane", 47);
        recordReader.addRecord(3, "Ilamaran", 27);
        recordReader.addRecord(4, "Jian", 14);
        recordReader.addRecord(5, "Sakura", 24);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(
                PutCassandraRecord.REL_SUCCESS, 1);

        assertEquals(5, getRecordsCount());
    }

    private int getRecordsCount() {
        SimpleStatement stmt = QueryBuilder
                .selectFrom(KEYSPACE, TABLE)
                .countAll()
                .build();

        ResultSet rs = session.execute(stmt);
        Row row = rs.one();

        dropRecords();
        return row != null ? (int) row.getLong(0) : 0;
    }

    private void dropRecords() {
        SimpleStatement truncate =
                QueryBuilder.truncate(KEYSPACE, TABLE).build();
        session.execute(truncate);
    }

    @AfterAll
    public static void shutdown() {
        session.execute("DROP TABLE IF EXISTS " + KEYSPACE + "." + TABLE);
        session.execute("DROP KEYSPACE " + KEYSPACE);
        session.close();
    }
}
