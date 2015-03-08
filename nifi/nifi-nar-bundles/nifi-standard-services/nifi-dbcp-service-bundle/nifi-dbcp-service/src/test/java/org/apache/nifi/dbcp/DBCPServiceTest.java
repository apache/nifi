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

import static org.apache.nifi.dbcp.DatabaseSystems.getDescriptor;
import static org.junit.Assert.*;

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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DBCPServiceTest {
    
	final static String DB_LOCATION = "/var/tmp/testdb";
	
	/**
	 *	Unknown database system.  
	 * 
	 */
//    @Test
    public void testUnknownDatabaseSystem() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        final Map<String, String> properties = new HashMap<String, String>();
        properties.put(DBCPConnectionPool.DATABASE_SYSTEM.getName(), "garbage");
        runner.addControllerService("test-bad2", service, properties);
        runner.assertNotValid(service);
    }
    
    /**
     *  Missing property values.
     */    
//    @Test
    public void testMissingPropertyValues() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        final Map<String, String> properties = new HashMap<String, String>();
        runner.addControllerService("test-bad1", service, properties);
        runner.assertNotValid(service);
    }

    /**
     * 	Test database connection using Derby.
     * Connect, create table, insert, select, drop table. 
     * 
     */
//    @Test
    public void testCreateInsertSelect() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("test-good1", service);
        
        // remove previous test database, if any
        File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        
        // Should setProperty call also generate DBCPConnectionPool.onPropertyModified() method call? 
        // It does not currently.
        
        // Some properties already should have JavaDB/Derby default values, let's set only missing values.
        
        runner.setProperty(service, DBCPConnectionPool.DB_HOST, "NA");	// Embedded Derby don't use host
        runner.setProperty(service, DBCPConnectionPool.DB_NAME, DB_LOCATION);
        runner.setProperty(service, DBCPConnectionPool.DB_USER, 	"tester");
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, "testerp");
        
        runner.enableControllerService(service);

        runner.assertValid(service);
        DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
        Assert.assertNotNull(dbcpService);
        Connection connection = dbcpService.getConnection();
        Assert.assertNotNull(connection);
        
        createInsertSelectDrop(connection);
        
        connection.close();		// return to pool
    }

    /**
     *  NB!!!!
     * 	Prerequisite: file should be present in /var/tmp/mariadb-java-client-1.1.7.jar
     * 	Prerequisite: access to running MariaDb database server
     * 
     * 	Test database connection using external JDBC jar located by URL.
     * Connect, create table, insert, select, drop table. 
     * 
     */
//    @Test
    public void testExternalJDBCDriverUsage() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("test-external-jar", service);
        
        DatabaseSystemDescriptor mariaDb = getDescriptor("MariaDB");
        assertNotNull(mariaDb);
        
        // Set MariaDB properties values.
        runner.setProperty(service, DBCPConnectionPool.DATABASE_SYSTEM, mariaDb.getValue());
        runner.setProperty(service, DBCPConnectionPool.DB_PORT, 		mariaDb.defaultPort.toString());
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, 	mariaDb.driverClassName);
        runner.setProperty(service, DBCPConnectionPool.DB_DRIVER_JAR_URL, "file:///var/tmp/mariadb-java-client-1.1.7.jar");
        
        
        runner.setProperty(service, DBCPConnectionPool.DB_HOST, "127.0.0.1");	// localhost
        runner.setProperty(service, DBCPConnectionPool.DB_NAME, "testdb");
        runner.setProperty(service, DBCPConnectionPool.DB_USER, 	"tester");
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, "testerp");
        
        runner.enableControllerService(service);

        runner.assertValid(service);
        DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-external-jar");
        Assert.assertNotNull(dbcpService);
        Connection connection = dbcpService.getConnection();
        Assert.assertNotNull(connection);
        
        createInsertSelectDrop(connection);
        
        connection.close();		// return to pool
    }

    
    @Rule
    public ExpectedException exception = ExpectedException.none();
    
    /**
     * 	Test get database connection using Derby.
     * Get many times, after a while pool should not contain any available connection
     * and getConnection should fail.    
     */
//    @Test
    public void testExhaustPool() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("test-exhaust", service);
        
        // remove previous test database, if any
        File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        
        runner.setProperty(service, DBCPConnectionPool.DB_HOST, "NA");	// Embedded Derby don't use host
        runner.setProperty(service, DBCPConnectionPool.DB_NAME, DB_LOCATION);
        runner.setProperty(service, DBCPConnectionPool.DB_USER, 	"tester");
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, "testerp");
        
        runner.enableControllerService(service);

        runner.assertValid(service);
        DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-exhaust");
        Assert.assertNotNull(dbcpService);
        
        exception.expect(ProcessException.class);
        exception.expectMessage("Cannot get a connection, pool error Timeout waiting for idle object");
        for (int i = 0; i < 100; i++) {
            Connection connection = dbcpService.getConnection();
            Assert.assertNotNull(connection);
		}
    }

    /**
     * 	Test get database connection using Derby.
     * Get many times, release immediately
     * and getConnection should not fail.    
     */
//    @Test
    public void testGetManyNormal() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool service = new DBCPConnectionPool();
        runner.addControllerService("test-exhaust", service);
        
        // remove previous test database, if any
        File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        
        runner.setProperty(service, DBCPConnectionPool.DB_HOST, "NA");	// Embedded Derby don't use host
        runner.setProperty(service, DBCPConnectionPool.DB_NAME, DB_LOCATION);
        runner.setProperty(service, DBCPConnectionPool.DB_USER, 	"tester");
        runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, "testerp");
        
        runner.enableControllerService(service);

        runner.assertValid(service);
        DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-exhaust");
        Assert.assertNotNull(dbcpService);
        
        for (int i = 0; i < 1000; i++) {
            Connection connection = dbcpService.getConnection();
            Assert.assertNotNull(connection);
            connection.close();
		}
    }
    
    
//    @Test
    public void testDriverLoad() throws ClassNotFoundException {
    	Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    	assertNotNull(clazz);
    }
    
    /**
     *  NB!!!!
     * 	Prerequisite: file should be present in /var/tmp/mariadb-java-client-1.1.7.jar
     */
    @Test
    public void testURLClassLoader() throws ClassNotFoundException, MalformedURLException, SQLException, InstantiationException, IllegalAccessException {
    	
    	URL url = new URL("file:///var/tmp/mariadb-java-client-1.1.7.jar");
    	URL[] urls = new URL[] { url };
    	
    	ClassLoader parent = Thread.currentThread().getContextClassLoader();
    	URLClassLoader ucl = new URLClassLoader(urls,parent);
    	
    	Class<?> clazz = Class.forName("org.mariadb.jdbc.Driver", true, ucl);
    	assertNotNull(clazz);
    	
    	Driver driver = (Driver) clazz.newInstance();
    	
    	// Driver is found when using URL ClassLoader
    	assertTrue( isDriverAllowed(driver, ucl) );
    	
    	// Driver is not found when using parent ClassLoader
    	// unfortunately DriverManager will use caller ClassLoadar and driver is not found !!!  
    	assertTrue( isDriverAllowed(driver, parent) );
    	
//    	DriverManager.registerDriver( (Driver) clazz.newInstance());
    	Enumeration<Driver> drivers = DriverManager.getDrivers();
    	while (drivers.hasMoreElements()) {
			driver = (Driver) drivers.nextElement();
			System.out.println(driver);
		}
    	
    	
   // 	Driver driver = DriverManager.getDriver("jdbc:mariadb://127.0.0.1:3306/testdb");
   // 	assertNotNull(driver);
    }

    
	String createTable = "create table restaurants(id integer, name varchar(20), city varchar(50))";
	String dropTable = "drop table restaurants";
    
    protected void createInsertSelectDrop( Connection con) throws SQLException {
    	
    	Statement st = con.createStatement();
    	
    	try {
        	st.executeUpdate(dropTable);
		} catch (Exception e) {
			// table may not exist, this is not serious problem.
		}
    	
		st.executeUpdate(createTable);
    	
    	st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
    	st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
    	st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

    	int nrOfRows = 0;
    	ResultSet resultSet = st.executeQuery("select * from restaurants");
    	while (resultSet.next())
    		nrOfRows++;
    	assertEquals(3, nrOfRows);

    	st.close();
    }

    //==================================== problem solving - no suitable driver found, mariadb =========================================
    
    private static boolean isDriverAllowed(Driver driver, ClassLoader classLoader) {
        boolean result = false;
        if(driver != null) {
            Class<?> aClass = null;
            try {
                aClass =  Class.forName(driver.getClass().getName(), true, classLoader);
            } catch (Exception ex) {
            	System.out.println(ex);
                result = false;
            }

             result = ( aClass == driver.getClass() ) ? true : false;
        }

        return result;
    }
    
    
    
    
}
