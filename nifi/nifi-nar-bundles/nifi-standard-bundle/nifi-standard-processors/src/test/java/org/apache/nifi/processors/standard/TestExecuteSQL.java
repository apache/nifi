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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.TestJdbcHugeStream;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.fusesource.hawtbuf.ByteArrayInputStream;
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
    public void testNoTimeLimit() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTrigger(null);
    }

    @Test
    public void testQueryTimeout() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        // Does to seem to have any effect when using embedded Derby
        invokeOnTrigger(1); // 1 second max time
    }

    public void invokeOnTrigger(final Integer queryTimeout) throws InitializationException, ClassNotFoundException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteSQL.class);

        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner.addControllerService("dbcp", dbcp, dbcpProperties);

        runner.enableControllerService(dbcp);
        runner.setProperty(ExecuteSQL.DBCP_SERVICE, "dbcp");

        if (queryTimeout != null) {
            runner.setProperty(ExecuteSQL.QUERY_TIMEOUT, queryTimeout.toString() + " secs");
        }

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = dbcp.getConnection();
        TestJdbcHugeStream.loadTestData2Database(con, 100, 2000, 1000);
        LOGGER.info("test data loaded");

        // ResultSet size will be 1x2000x1000 = 2 000 000 rows
        // because of where PER.ID = ${person.id}
        final int nrOfRows = 2000000;
        final String query = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID = ${person.id}";

        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, query);

        // incoming FlowFile content is not used, but attributes are used
        final Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("person.id", "10");
        runner.enqueue("Hello".getBytes(), attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);

        // read all Avro records and verify created FlowFile contains 1000000
        // records
        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS);
        final InputStream in = new ByteArrayInputStream(flowfiles.get(0).toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(in, datumReader);
        GenericRecord record = null;
        long recordsFromStream = 0;
        while (dataFileReader.hasNext()) {
            // Reuse record object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            record = dataFileReader.next(record);
            recordsFromStream += 1;
        }

        LOGGER.info("total nr of records from stream: " + recordsFromStream);
        assertEquals(nrOfRows, recordsFromStream);
        dataFileReader.close();
    }

    /**
     * Simple implementation only for ExecuteSQL processor testing.
     *
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

}
