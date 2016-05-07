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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFilterCSVColumns {

    private static final Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.FilterCSVColumns", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestFilterCSVColumns", "debug");
        LOGGER = LoggerFactory.getLogger(TestFilterCSVColumns.class);
    }

    @Test
    public void testTransformSimple() throws InitializationException, IOException, SQLException {
        String sql = "select first_name, last_name, company_name, address, city from CSV.A where city='New York'";

        Path inpath = Paths.get("src/test/resources/TestFilterCSVColumns/US500.csv");
        InputStream in = new FileInputStream(inpath.toFile());

        ResultSet resultSet = FilterCSVColumns.transform(in, sql);

        int nrofColumns = resultSet.getMetaData().getColumnCount();

        for (int i = 1; i <= nrofColumns; i++) {
            System.out.print(resultSet.getMetaData().getColumnLabel(i) + "      ");
        }
        System.out.println();

        while (resultSet.next()) {
            for (int i = 1; i <= nrofColumns; i++) {
                System.out.print(resultSet.getString(i)+ "  ");
            }
            System.out.println();
        }
    }

    @Test
    public void testTransformCalc() throws InitializationException, IOException, SQLException {
        String sql = "select ID, AMOUNT1+AMOUNT2+AMOUNT3 as TOTAL from CSV.A where ID=100";

        Path inpath = Paths.get("src/test/resources/TestFilterCSVColumns/Numeric.csv");
        InputStream in = new FileInputStream(inpath.toFile());

        ResultSet resultSet = FilterCSVColumns.transform(in, sql);

        int nrofColumns = resultSet.getMetaData().getColumnCount();

        for (int i = 1; i <= nrofColumns; i++) {
            System.out.print(resultSet.getMetaData().getColumnLabel(i) + "      ");
        }
        System.out.println();

        while (resultSet.next()) {
            for (int i = 1; i <= nrofColumns; i++) {
                System.out.print(resultSet.getString(i)+ "  ");
            }
            double total = resultSet.getDouble("TOTAL");
            System.out.println();
            assertEquals(90.75, total, 0.0001);
        }
    }

    @Test
    public void testSimpleTypeless() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(FilterCSVColumns.class);
        String sql = "select first_name, last_name, company_name, address, city from CSV.A where city='New York'";
        runner.setProperty(FilterCSVColumns.SQL_SELECT, sql);

        runner.enqueue(Paths.get("src/test/resources/TestFilterCSVColumns/US500_typeless.csv"));
        runner.run();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile);
            System.out.println(new String(flowFile.toByteArray()));
        }
    }

}
