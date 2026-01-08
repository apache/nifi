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

package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System tests that verify JSON marshalling of complex objects across ClassLoader boundaries
 * when invoking ConnectorMethods on Processors.
 */
public class ConnectorMethodMarshallingIT extends NiFiSystemIT {

    @Test
    public void testComplexObjectMarshalling() throws NiFiClientException, IOException, InterruptedException {
        final File outputFile = new File("target/calculate-result.txt");
        if (outputFile.exists()) {
            assertTrue(outputFile.delete(), "Failed to delete existing output file");
        }

        final ConnectorEntity connector = getClientUtil().createConnector("CalculateConnector");
        assertNotNull(connector);

        getClientUtil().configureConnector(connector, "Calculation", Map.of(
            "Operand 1", "10",
            "Operand 2", "5",
            "Operation", "ADD",
            "Output File", outputFile.getAbsolutePath()
        ));

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());

        assertTrue(outputFile.exists(), "Output file was not created");
        final String fileContents = Files.readString(outputFile.toPath(), StandardCharsets.UTF_8);
        final String[] lines = fileContents.split("\n");
        assertEquals(4, lines.length, "Output file should contain 4 lines");
        assertEquals("10", lines[0], "First line should be operand1");
        assertEquals("5", lines[1], "Second line should be operand2");
        assertEquals("ADD", lines[2], "Third line should be the operation");
        assertEquals("15", lines[3], "Fourth line should be the result (10 + 5 = 15)");
    }
}

