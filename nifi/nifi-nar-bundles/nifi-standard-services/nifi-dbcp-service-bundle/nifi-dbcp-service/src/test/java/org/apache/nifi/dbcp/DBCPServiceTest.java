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

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.activation.DataSource;

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
    @Test
    public void testBad1() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPServiceApacheDBCP14 service = new DBCPServiceApacheDBCP14();
        final Map<String, String> properties = new HashMap<String, String>();
        properties.put(DBCPServiceApacheDBCP14.DATABASE_SYSTEM.getName(), "garbage");
        runner.addControllerService("test-bad2", service, properties);
        runner.assertNotValid(service);
    }
    
    /**
     *  Missing property values.
     */    
    @Test
    public void testGood1() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPServiceApacheDBCP14 service = new DBCPServiceApacheDBCP14();
        final Map<String, String> properties = new HashMap<String, String>();
        runner.addControllerService("test-bad1", service, properties);
        runner.assertNotValid(service);
    }

    /**
     * 	Test database connection using Derby.
     * Connect, create table, insert, select, drop table. 
     * 
     */
    @Test
    public void testGood2() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPServiceApacheDBCP14 service = new DBCPServiceApacheDBCP14();
        runner.addControllerService("test-good1", service);
        
        // remove previous test database, if any
        File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        
        // Should setProperty call also generate DBCPServiceApacheDBCP14.onPropertyModified() method call? 
        // It does not currently.
        
        // Some properties already should have JavaDB/Derby default values, let's set only missing values.
        
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_HOST, "NA");	// Embedded Derby don't use host
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_NAME, DB_LOCATION);
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_USER, 	"tester");
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_PASSWORD, "testerp");
        
        runner.enableControllerService(service);

        runner.assertValid(service);
        DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
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
    @Test
    public void testExhaustPool() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPServiceApacheDBCP14 service = new DBCPServiceApacheDBCP14();
        runner.addControllerService("test-exhaust", service);
        
        // remove previous test database, if any
        File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_HOST, "NA");	// Embedded Derby don't use host
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_NAME, DB_LOCATION);
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_USER, 	"tester");
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_PASSWORD, "testerp");
        
        runner.enableControllerService(service);

        runner.assertValid(service);
        DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-exhaust");
        Assert.assertNotNull(dbcpService);
        
        exception.expect(ProcessException.class);
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
    @Test
    public void testGetMany() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPServiceApacheDBCP14 service = new DBCPServiceApacheDBCP14();
        runner.addControllerService("test-exhaust", service);
        
        // remove previous test database, if any
        File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
        
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_HOST, "NA");	// Embedded Derby don't use host
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_NAME, DB_LOCATION);
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_USER, 	"tester");
        runner.setProperty(service, DBCPServiceApacheDBCP14.DB_PASSWORD, "testerp");
        
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
    
    
    @Test
    public void testDriverLaod() throws ClassNotFoundException {
    	Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    	assertNotNull(clazz);
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

}
