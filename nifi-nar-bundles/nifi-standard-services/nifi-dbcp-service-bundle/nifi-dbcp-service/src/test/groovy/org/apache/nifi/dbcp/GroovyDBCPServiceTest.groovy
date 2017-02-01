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
package org.apache.nifi.dbcp

import org.apache.nifi.reporting.InitializationException
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test

import java.sql.Connection
import java.sql.SQLException

import static org.apache.nifi.dbcp.DBCPConnectionPool.*

/**
 * Groovy unit tests for the DBCPService module.
 */
class GroovyDBCPServiceTest {

    final static String DB_LOCATION = "target/db"

    @BeforeClass
    static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log")
    }

    /**
     * Test dynamic connection properties.
     */
    @Test
    void testDynamicProperties() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor)
        final DBCPConnectionPool service = new DBCPConnectionPool()
        runner.addControllerService("test-dynamic-properties", service)

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        dbLocation.deleteDir()

        // set embedded Derby database connection url
        runner.setProperty(service, DATABASE_URL, "jdbc:derby:" + DB_LOCATION)
        runner.setProperty(service, DB_USER, "tester")
        runner.setProperty(service, DB_PASSWORD, "testerp")
        runner.setProperty(service, DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver")
        runner.setProperty(service, "create", "true")

        runner.enableControllerService(service)

        runner.assertValid(service)
        final DBCPService dbcpService = (DBCPService) runner.processContext.controllerServiceLookup.getControllerService("test-dynamic-properties")
        Assert.assertNotNull(dbcpService)

        2.times {
            final Connection connection = dbcpService.getConnection()
            Assert.assertNotNull(connection)
            connection.close() // will return connection to pool
        }
    }
}
