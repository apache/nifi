/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.basex;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;

public class TestEvaluateBaseXQuery {
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(EvaluateBaseXQuery.class);
    }

    @Test
    public void testSelectAllProductNamesFromWarehouse() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "//product/name");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);

        testRunner.enqueue(Paths.get("src/test/resources/xml/warehouses.xml"));
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("<name>Product 1</name>\n<name>Product 2</name>\n<name>Product 3</name>\n<name>Product 4</name>");
    }

    @Test
    public void testFilterProductsByQuantity() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT,
                "for $p in //product where xs:integer($p/quantity) > 10 return $p/name/text()");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);

        testRunner.enqueue(Paths.get("src/test/resources/xml/warehouses.xml"));
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("Product 2\nProduct 4");
    }

    @Test
    public void testGetEmployeesFromDepartment1() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT,
                "for $e in //employee where $e/department = 'Department 1' return $e/name/text()");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);

        testRunner.enqueue(Paths.get("src/test/resources/xml/employees.xml"));
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("Employee A\nEmployee C");
    }

    @Test
    public void testGetTotalHoursWorked() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT,
                "sum(//employee/hoursWorked)");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);

        testRunner.enqueue(Paths.get("src/test/resources/xml/employees.xml"));
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("560");
    }

    @Test
    public void testNoMatchingSectionReturnsEmpty() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "//section[@id='Z']/product/name/text()");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);
        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue(Paths.get("src/test/resources/xml/warehouses.xml"));
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("");
    }

    @Test
    public void testOutputToAttribute() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT,
                "sum(//employee/hoursWorked)");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);
        testRunner.setProperty(EvaluateBaseXQuery.OUTPUT_BODY_ATTRIBUTE_NAME, "totalHoursWorked");
        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue(Paths.get("src/test/resources/xml/employees.xml"));
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_ORIGINAL, 1);
        MockFlowFile originalOut = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_ORIGINAL).get(0);

        originalOut.assertAttributeEquals("totalHoursWorked", "560");
        originalOut.assertContentEquals(Paths.get("src/test/resources/xml/employees.xml"));
    }

    @Test
    public void testDoNotMapStrategy() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT,
                "for $p in //product return $p/name/text()");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.DO_NOT_MAP);
        testRunner.enqueue(Paths.get("src/test/resources/xml/products_static.xml"));
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("Table\nChair");
    }

    @Test
    public void testMappedVariable() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT,
                "for $p in //product where $p/id = $targetId return $p/name/text()");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_LIST);
        testRunner.setProperty(EvaluateBaseXQuery.MAPPING_LIST, "targetId");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("targetId", "1");
        testRunner.enqueue(Paths.get("src/test/resources/xml/products.xml"),attributes);

        testRunner.run();
        MockFlowFile successOut = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        successOut.assertContentEquals("Keyboard");
    }
    @Test
    public void testEmptyInput() {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "//product/name");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);

        testRunner.enqueue(new byte[0]); // pusty plik
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_FAILURE, 1);
    }
    @Test
    public void testMissingElementQuery() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "//nonexistent");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);

        testRunner.enqueue(Paths.get("src/test/resources/xml/products.xml"));
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("");
    }

    @Test
    public void testMappedVariableTwoAttributes() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT,
                "for $p in //product where $p/id = $targetId and $p/name/text() = $targetName return $p");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_LIST);
        testRunner.setProperty(EvaluateBaseXQuery.MAPPING_LIST, "targetId,targetName");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("targetId", "1");
        attributes.put("targetName", "Keyboard");
        testRunner.enqueue(Paths.get("src/test/resources/xml/products.xml"),attributes);
        testRunner.run();
        MockFlowFile successOut = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        successOut.assertContentEquals("<product><id>1</id><name>Keyboard</name></product>");
    }


}