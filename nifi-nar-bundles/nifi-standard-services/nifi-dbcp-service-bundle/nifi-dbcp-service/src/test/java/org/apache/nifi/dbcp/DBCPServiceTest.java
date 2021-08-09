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
package org.apache.nifi.dbcp;

import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.kerberos.MockKerberosCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DBCPServiceTest {
    private static final String SERVICE_ID = DBCPConnectionPool.class.getName();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    private TestRunner runner;

    private DBCPConnectionPool service;

    @BeforeClass
    public static void setDerbyLog() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Before
    public void setService() throws InitializationException {
        service = new DBCPConnectionPool();
        runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.addControllerService(SERVICE_ID, service);

        final String databasePath = new File(tempFolder.getRoot(), "db").getPath();
        final String databaseUrl = String.format("jdbc:derby:%s;create=true", databasePath);
        runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, databaseUrl);
        runner.setProperty(service, DBCPConnectionPool.DB_USER, String.class.getSimpleName());
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, String.class.getName());
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
    }

    @Test
    public void testNotValidWithNegativeMinIdleProperty() {
        runner.setProperty(service, DBCPConnectionPool.MIN_IDLE, "-1");
        runner.assertNotValid(service);
    }

    @Test
    public void testGetConnectionDynamicProperty() throws SQLException {
        assertConnectionNotNullDynamicProperty("create", "true");
    }

    @Test
    public void testGetConnectionDynamicPropertyExpressionLanguageSupported() throws SQLException {
        assertConnectionNotNullDynamicProperty("create", "${literal(1):gt(0)}");
    }

    @Test
    public void testGetConnectionDynamicPropertySensitivePrefixSupported() throws SQLException {
        assertConnectionNotNullDynamicProperty("SENSITIVE.create", "true");
    }

    @Test
    public void testGetConnectionExecuteStatements() throws SQLException {
        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            Assert.assertNotNull("Connection not found", connection);

            try (final Statement st = connection.createStatement()) {
                st.executeUpdate("create table restaurants(id integer, name varchar(20), city varchar(50))");

                st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
                st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
                st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

                try (final ResultSet resultSet = st.executeQuery("select count(*) AS total_rows from restaurants")) {
                    assertTrue("Result Set Row not found", resultSet.next());
                    final int rows = resultSet.getInt(1);
                    assertEquals(3, rows);
                }
            }
        }
    }

    @Test
    public void testGetConnectionKerberosLoginException() throws InitializationException {
        final KerberosCredentialsService kerberosCredentialsService = new MockKerberosCredentialsService();
        final String kerberosServiceId = "kcs";
        runner.addControllerService(kerberosServiceId, kerberosCredentialsService);
        runner.setProperty(kerberosCredentialsService, MockKerberosCredentialsService.PRINCIPAL, "bad@PRINCIPAL.COM");
        runner.setProperty(kerberosCredentialsService, MockKerberosCredentialsService.KEYTAB, "src/test/resources/fake.keytab");
        runner.enableControllerService(kerberosCredentialsService);

        // set fake Derby database connection url
        runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, "jdbc:derby://localhost:1527/NoDB");
        // Use the client driver here rather than the embedded one, as it will generate a ConnectException for the test
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.ClientDriver");
        runner.setProperty(service, DBCPConnectionPool.KERBEROS_CREDENTIALS_SERVICE, kerberosServiceId);

        try {
            runner.enableControllerService(service);
        } catch (AssertionError ae) {
            // Ignore, this happens because it tries to do the initial Kerberos login
        }

        runner.assertValid(service);
        Assert.assertThrows(ProcessException.class, service::getConnection);
    }

    @Test
    public void testGetConnection() throws SQLException {
        runner.setProperty(service, DBCPConnectionPool.MAX_TOTAL_CONNECTIONS, "2");
        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            Assert.assertNotNull("First Connection not found", connection);
        }
        try (final Connection connection = service.getConnection()) {
            Assert.assertNotNull("Second Connection not found", connection);
        }
    }

    @Test
    public void testGetConnectionMaxTotalConnectionsExceeded() {
        runner.setProperty(service, DBCPConnectionPool.MAX_TOTAL_CONNECTIONS, "1");
        runner.setProperty(service, DBCPConnectionPool.MAX_WAIT_TIME, "1 ms");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final Connection connection = service.getConnection();
        Assert.assertNotNull(connection);
        Assert.assertThrows(ProcessException.class, service::getConnection);
    }

    @Test
    public void testGetDataSourceProperties() throws SQLException {
        runner.setProperty(service, DBCPConnectionPool.MAX_WAIT_TIME, "-1");
        runner.setProperty(service, DBCPConnectionPool.MAX_IDLE, "6");
        runner.setProperty(service, DBCPConnectionPool.MIN_IDLE, "4");
        runner.setProperty(service, DBCPConnectionPool.MAX_CONN_LIFETIME, "1 secs");
        runner.setProperty(service, DBCPConnectionPool.EVICTION_RUN_PERIOD, "1 secs");
        runner.setProperty(service, DBCPConnectionPool.MIN_EVICTABLE_IDLE_TIME, "1 secs");
        runner.setProperty(service, DBCPConnectionPool.SOFT_MIN_EVICTABLE_IDLE_TIME, "1 secs");

        runner.enableControllerService(service);

        Assert.assertEquals(6, service.getDataSource().getMaxIdle());
        Assert.assertEquals(4, service.getDataSource().getMinIdle());
        Assert.assertEquals(1000, service.getDataSource().getMaxConnLifetimeMillis());
        Assert.assertEquals(1000, service.getDataSource().getTimeBetweenEvictionRunsMillis());
        Assert.assertEquals(1000, service.getDataSource().getMinEvictableIdleTimeMillis());
        Assert.assertEquals(1000, service.getDataSource().getSoftMinEvictableIdleTimeMillis());

        service.getDataSource().close();
    }

    @Test
    public void testGetDataSourceIdleProperties() throws SQLException {
        runner.setProperty(service, DBCPConnectionPool.MAX_WAIT_TIME, "${max.wait.time}");
        runner.setProperty(service, DBCPConnectionPool.MAX_TOTAL_CONNECTIONS, "${max.total.connections}");
        runner.setProperty(service, DBCPConnectionPool.MAX_IDLE, "${max.idle}");
        runner.setProperty(service, DBCPConnectionPool.MIN_IDLE, "${min.idle}");
        runner.setProperty(service, DBCPConnectionPool.MAX_CONN_LIFETIME, "${max.conn.lifetime}");
        runner.setProperty(service, DBCPConnectionPool.EVICTION_RUN_PERIOD, "${eviction.run.period}");
        runner.setProperty(service, DBCPConnectionPool.MIN_EVICTABLE_IDLE_TIME, "${min.evictable.idle.time}");
        runner.setProperty(service, DBCPConnectionPool.SOFT_MIN_EVICTABLE_IDLE_TIME, "${soft.min.evictable.idle.time}");

        runner.setVariable("max.wait.time", "1 sec");
        runner.setVariable("max.total.connections", "7");
        runner.setVariable("max.idle", "4");
        runner.setVariable("min.idle", "1");
        runner.setVariable("max.conn.lifetime", "1000 millis");
        runner.setVariable("eviction.run.period", "100 millis");
        runner.setVariable("min.evictable.idle.time", "100 millis");
        runner.setVariable("soft.min.evictable.idle.time", "100 millis");

        runner.enableControllerService(service);

        ArrayList<Connection> connections = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            connections.add(service.getConnection());
        }

        Assert.assertEquals(6, service.getDataSource().getNumActive());

        connections.get(0).close();
        Assert.assertEquals(5, service.getDataSource().getNumActive());
        Assert.assertEquals(1, service.getDataSource().getNumIdle());

        connections.get(1).close();
        connections.get(2).close();
        connections.get(3).close();
        //now at max idle
        Assert.assertEquals(2, service.getDataSource().getNumActive());
        Assert.assertEquals(4, service.getDataSource().getNumIdle());

        //now a connection should get closed for real so that numIdle does not exceed maxIdle
        connections.get(4).close();
        Assert.assertEquals(4, service.getDataSource().getNumIdle());
        Assert.assertEquals(1, service.getDataSource().getNumActive());

        connections.get(5).close();
        Assert.assertEquals(4, service.getDataSource().getNumIdle());
        Assert.assertEquals(0, service.getDataSource().getNumActive());

        service.getDataSource().close();
    }

    private void assertConnectionNotNullDynamicProperty(final String propertyName, final String propertyValue) throws SQLException {
        runner.setProperty(service, propertyName, propertyValue);

        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection);
        }
    }
}
