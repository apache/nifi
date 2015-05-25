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
package org.apache.nifi.processors.standard;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestExecuteSQL {

    private static Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ExecuteSQL", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestExecuteSQL", "debug");
        LOGGER = LoggerFactory.getLogger(TestExecuteSQL.class);
    }

    final static String DB_LOCATION = "target/db";

    @BeforeClass
    public static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }
    
    @Test
    public void test1() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteSQL.class);
        
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();
        dbcpProperties.put("Database Host", "NA");    // Embedded Derby don't use host
        dbcpProperties.put("Database Port", "1");  // Embedded Derby don't use port, but must have value anyway
        dbcpProperties.put("Database Name", DB_LOCATION);
        dbcpProperties.put("Database User",     "tester");
        dbcpProperties.put("Password", "testerp");

        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        
        runner.setProperty(ExecuteSQL.DBCP_SERVICE, "dbcp");
        
        String query = "select "
        		+ "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
        		+ ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
        		+ ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
        		+ ", ROW_NUMBER() OVER () as rownr "
        		+ " from persons PER, products PRD, relationships REL";
        
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, query);
        runner.enableControllerService(dbcp);
        
        runner.enqueue("Hello".getBytes());

        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    /**
     * Simple implementation only for ExecuteSQL processor testing.
     *
     */
    class DBCPServiceSimpleImpl implements DBCPService {

		@Override
		public void initialize(ControllerServiceInitializationContext context) throws InitializationException { }

		@Override
		public Collection<ValidationResult> validate(ValidationContext context) { return null; }

		@Override
		public PropertyDescriptor getPropertyDescriptor(String name) { return null; }

		@Override
		public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) { }

		@Override
		public List<PropertyDescriptor> getPropertyDescriptors() { return null; }

		@Override
		public String getIdentifier() { return null; }

		@Override
		public Connection getConnection() throws ProcessException {
	        try {
				Class.forName("org.apache.derby.jdbc.EmbeddedDriver");        
				Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
				return con;
			} catch (Exception e) {
				throw new ProcessException("getConnection failed: " + e);
			}
		}    	
    }
    
}
