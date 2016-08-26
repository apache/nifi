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

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DBCPServiceTest {

    final static String DB_LOCATION = "target/db";

    @BeforeClass
    public static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    /**
     * Missing property values.
     */
    @Test
    public void testMissingPropertyValues() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        final Map<String, String> properties = new HashMap<String, String>();
        runner.addControllerService("test-bad1", service, properties);
        runner.assertNotValid(service);
    }

    /**
     * Test database connection using Derby. Connect, create table, insert, select, drop table.
     *
     */
    @Test
    public void testCreateInsertSelect() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("test-good1", service);

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // set embedded Derby database connection url
        runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, DBCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
        Assert.assertNotNull(dbcpService);
        final Connection connection = dbcpService.getConnection();
        Assert.assertNotNull(connection);

        createInsertSelectDrop(connection);

        connection.close(); // return to pool
    }

    /**
     * NB!!!! Prerequisite: file should be present in /var/tmp/mariadb-java-client-1.1.7.jar Prerequisite: access to running MariaDb database server
     *
     * Test database connection using external JDBC jar located by URL. Connect, create table, insert, select, drop table.
     *
     */
    @Ignore
    @Test
    public void testExternalJDBCDriverUsage() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("test-external-jar", service);

        // set MariaDB database connection url
        runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, "jdbc:mariadb://localhost:3306/" + "testdb");
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "org.mariadb.jdbc.Driver");
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVER_LOCATION, "file:///var/tmp/mariadb-java-client-1.1.7.jar");

        runner.setProperty(service, DBCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, "testerp");

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-external-jar");
        Assert.assertNotNull(dbcpService);
        final Connection connection = dbcpService.getConnection();
        Assert.assertNotNull(connection);

        createInsertSelectDrop(connection);

        connection.close(); // return to pool
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Test get database connection using Derby. Get many times, after a while pool should not contain any available connection and getConnection should fail.
     */
    @Test
    public void testExhaustPool() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("test-exhaust", service);

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // set embedded Derby database connection url
        runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, DBCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-exhaust");
        Assert.assertNotNull(dbcpService);

        exception.expect(ProcessException.class);
        exception.expectMessage("Cannot get a connection, pool error Timeout waiting for idle object");
        for (int i = 0; i < 100; i++) {
            final Connection connection = dbcpService.getConnection();
            Assert.assertNotNull(connection);
        }
    }

    /**
     * Test get database connection using Derby. Get many times, release immediately and getConnection should not fail.
     */
    @Test
    public void testGetManyNormal() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("test-exhaust", service);

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // set embedded Derby database connection url
        runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, DBCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-exhaust");
        Assert.assertNotNull(dbcpService);

        for (int i = 0; i < 1000; i++) {
            final Connection connection = dbcpService.getConnection();
            Assert.assertNotNull(connection);
            connection.close(); // will return connection to pool
        }
    }

    @Test
    public void testDriverLoad() throws ClassNotFoundException {
        final Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        assertNotNull(clazz);
    }

    /**
     * NB!!!! Prerequisite: file should be present in /var/tmp/mariadb-java-client-1.1.7.jar
     */
    @Test
    @Ignore("Intended only for local testing, not automated testing")
    public void testURLClassLoader() throws ClassNotFoundException, MalformedURLException, SQLException, InstantiationException, IllegalAccessException {

        final URL url = new URL("file:///var/tmp/mariadb-java-client-1.1.7.jar");
        final URL[] urls = new URL[] { url };

        final ClassLoader parent = Thread.currentThread().getContextClassLoader();
        final URLClassLoader ucl = new URLClassLoader(urls, parent);

        final Class<?> clazz = Class.forName("org.mariadb.jdbc.Driver", true, ucl);
        assertNotNull(clazz);

        final Driver driver = (Driver) clazz.newInstance();
        final Driver shim = new DriverShim(driver);
        DriverManager.registerDriver(shim);

        final Driver driver2 = DriverManager.getDriver("jdbc:mariadb://localhost:3306/testdb");
        assertNotNull(driver2);
    }

    /**
     * NB!!!! Prerequisite: file should be present in /var/tmp/mariadb-java-client-1.1.7.jar Prerequisite: access to running MariaDb database server
     */
    @Test
    @Ignore("Intended only for local testing, not automated testing")
    public void testURLClassLoaderGetConnection() throws ClassNotFoundException, MalformedURLException, SQLException, InstantiationException, IllegalAccessException {

        final URL url = new URL("file:///var/tmp/mariadb-java-client-1.1.7.jar");
        final URL[] urls = new URL[] { url };

        final ClassLoader parent = Thread.currentThread().getContextClassLoader();
        final URLClassLoader ucl = new URLClassLoader(urls, parent);

        final Class<?> clazz = Class.forName("org.mariadb.jdbc.Driver", true, ucl);
        assertNotNull(clazz);

        final Driver driver = (Driver) clazz.newInstance();
        final Driver shim = new DriverShim(driver);
        DriverManager.registerDriver(shim);

        final Driver driver2 = DriverManager.getDriver("jdbc:mariadb://localhost:3306/testdb");
        assertNotNull(driver2);

        final Connection connection = DriverManager.getConnection("jdbc:mariadb://localhost:3306/testdb", "tester", "testerp");
        assertNotNull(connection);
        connection.close();

        DriverManager.deregisterDriver(shim);
    }

    String createTable = "create table restaurants(id integer, name varchar(20), city varchar(50))";
    String dropTable = "drop table restaurants";

    protected void createInsertSelectDrop(Connection con) throws SQLException {

        final Statement st = con.createStatement();

        try {
            st.executeUpdate(dropTable);
        } catch (final Exception e) {
            // table may not exist, this is not serious problem.
        }

        st.executeUpdate(createTable);

        st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
        st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
        st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

        int nrOfRows = 0;
        final ResultSet resultSet = st.executeQuery("select * from restaurants");
        while (resultSet.next())
            nrOfRows++;
        assertEquals(3, nrOfRows);

        st.close();
    }

}
