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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class UpdateCassandraIT {
    private static Cluster cluster;
    private static Session session;

    private static final String KEYSPACE = "sample_keyspace";
    private static final String TABLE = "sample_table_for_delete";
    private static final String HOST = "localhost";
    private static final int PORT = 9042;

    @BeforeClass
    public static void beforeClass() {
        cluster = Cluster.builder().addContactPoint(HOST).withPort(PORT).build();
        session = cluster.connect();

        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};";
        String createTable = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + "(id int PRIMARY KEY, name text, age int);";

        session.execute(createKeyspace);
        session.execute(createTable);
    }

    @AfterClass
    public static void after() {
        String dropKeyspace = "DROP KEYSPACE " + KEYSPACE;
        String dropTable = "DROP TABLE IF EXISTS " + KEYSPACE + "." + TABLE;

        session.execute(dropTable);
        session.execute(dropKeyspace);

        session.close();
        cluster.close();
    }

    private TestRunner runner;

    private static final String SIMPLE_DELETE = "DELETE FROM sample_keyspace.sample_table_for_delete WHERE id = 2;";

    @Before
    public void setup() {
        session.execute("INSERT INTO sample_keyspace.sample_table_for_delete(id,name,age) VALUES (1, 'john.smith', 30);");
        session.execute("INSERT INTO sample_keyspace.sample_table_for_delete(id,name,age) VALUES (2, 'jane.doe', 25);");

        runner = TestRunners.newTestRunner(UpdateCassandra.class);
        runner.setProperty(UpdateCassandra.CONTACT_POINTS, "localhost:9042");
        runner.setProperty(UpdateCassandra.KEYSPACE, KEYSPACE);
    }

    @After
    public void teardown() {
        session.execute("TRUNCATE TABLE sample_keyspace.sample_table_for_delete;");
    }

    @Test
    public void testBodySimple() {
        runner.assertValid();
        runner.enqueue(SIMPLE_DELETE);
        runner.run();

        runner.assertTransferCount(UpdateCassandra.REL_FAILURE, 0);
        runner.assertTransferCount(UpdateCassandra.REL_SUCCESS, 1);

        long count = count();
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testQuerySimpleNoConnection() {
        runner.setIncomingConnection(false);
        runner.setProperty(UpdateCassandra.QUERY, SIMPLE_DELETE);
        runner.run();

        runner.assertTransferCount(UpdateCassandra.REL_FAILURE, 0);
        runner.assertTransferCount(UpdateCassandra.REL_SUCCESS, 0);

        long count = count();
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testDeleteParams() {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("cql.args.1.type", "int");
        attrs.put("cql.args.1.value", "2");
        runner.enqueue("DELETE FROM sample_keyspace.sample_table_for_delete WHERE id = ?", attrs);
        runner.run();

        runner.assertTransferCount(UpdateCassandra.REL_FAILURE, 0);
        runner.assertTransferCount(UpdateCassandra.REL_SUCCESS, 1);

        long count = count();
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testUpdate() {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("cql.args.1.type", "int");
        attrs.put("cql.args.1.value", "1");
        runner.enqueue("UPDATE sample_keyspace.sample_table_for_delete SET name = 'jack.ryan' WHERE id = ?", attrs);
        runner.run();

        runner.assertTransferCount(UpdateCassandra.REL_FAILURE, 0);
        runner.assertTransferCount(UpdateCassandra.REL_SUCCESS, 1);
    }

    private long count() {
        ResultSet resultSet = session.execute("SELECT count(*) AS cnt FROM sample_keyspace.sample_table_for_delete");
        return resultSet.one().getLong("cnt");
    }
}
