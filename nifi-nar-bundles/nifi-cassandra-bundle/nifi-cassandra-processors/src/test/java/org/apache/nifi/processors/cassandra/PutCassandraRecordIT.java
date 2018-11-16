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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class PutCassandraRecordIT {

    private static TestRunner testRunner;
    private static MockRecordParser recordReader;

    private static Cluster cluster;
    private static Session session;

    private static final String KEYSPACE = "sample_keyspace";
    private static final String TABLE = "sample_table";
    private static final String HOST = "localhost";
    private static final int PORT = 9042;

    @BeforeClass
    public static void setup() throws InitializationException {
        recordReader = new MockRecordParser();
        testRunner = TestRunners.newTestRunner(PutCassandraRecord.class);

        testRunner.setProperty(PutCassandraRecord.RECORD_READER_FACTORY, "reader");
        testRunner.setProperty(PutCassandraRecord.CONTACT_POINTS, HOST + ":" + PORT);
        testRunner.setProperty(PutCassandraRecord.KEYSPACE, KEYSPACE);
        testRunner.setProperty(PutCassandraRecord.TABLE, TABLE);
        testRunner.setProperty(PutCassandraRecord.CONSISTENCY_LEVEL, "SERIAL");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, "LOGGED");
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);

        cluster = Cluster.builder().addContactPoint(HOST).withPort(PORT).build();
        session = cluster.connect();

        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};";
        String createTable = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + "(id int PRIMARY KEY, name text, age int);";

        session.execute(createKeyspace);
        session.execute(createTable);
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

        testRunner.assertAllFlowFilesTransferred(PutCassandraRecord.REL_SUCCESS, 1);
        Assert.assertEquals(5, getRecordsCount());
    }

    private int getRecordsCount() {
        Select selectQuery = QueryBuilder.select().all().from(KEYSPACE, TABLE);
        ResultSet result = session.execute(selectQuery);

        List<Integer> resultsList = result.all()
                .stream()
                .map(r -> r.getInt(0))
                .collect(Collectors.toList());

        dropRecords();
        return resultsList.size();
    }

    private void dropRecords() {
        Truncate query = QueryBuilder.truncate(KEYSPACE, TABLE);
        session.execute(query);
    }

    @AfterClass
    public static void shutdown() {
        String dropKeyspace = "DROP KEYSPACE " + KEYSPACE;
        String dropTable = "DROP TABLE IF EXISTS " + KEYSPACE + "." + TABLE;

        session.execute(dropTable);
        session.execute(dropKeyspace);

        session.close();
        cluster.close();
    }
}
