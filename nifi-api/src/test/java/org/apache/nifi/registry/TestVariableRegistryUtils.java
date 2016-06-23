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
package org.apache.nifi.registry;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestVariableRegistryUtils {

    @Test
    public void testCreateSystemVariableRegistry(){
        System.setProperty("fake","test");
        VariableRegistry variableRegistry = VariableRegistryUtils.createSystemVariableRegistry();
        Map<String,String> variables = variableRegistry.getVariables();
        assertTrue(variables.containsKey("PATH"));
        assertTrue(variables.get("fake").equals("test"));
    }

    @Test
    public void testCreateCustomVariableRegistry(){
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        Path[] paths = {fooPath,testPath};
        System.setProperty("fake","test");
        VariableRegistry variableRegistry = VariableRegistryUtils.createCustomVariableRegistry(paths);
        Map<String,String> variables = variableRegistry.getVariables();
        assertTrue(variables.containsKey("PATH"));
        assertTrue(variables.containsKey("fake.property.3"));
        assertTrue(variables.get("fake").equals("test"));
        assertTrue(variables.get("fake.property.3").equals("test me out 3, test me out 4"));
    }

    @Test
    public void testCreateFlowVariableRegistry(){
        System.setProperty("fake","test");
        FlowFile flowFile = createFlowFile();

        VariableRegistry variableRegistry = VariableRegistryUtils.createSystemVariableRegistry();
        VariableRegistry populatedRegistry = VariableRegistryUtils.createFlowVariableRegistry(variableRegistry,flowFile,null);
        Map<String,String> variables = populatedRegistry.getVariables();
        assertTrue(variables.containsKey("PATH"));
        assertTrue(variables.get("fake").equals("test"));
        assertTrue(variables.get("flowFileId").equals("1"));
        assertTrue(variables.get("fileSize").equals("50"));
        assertTrue(variables.get("entryDate").equals("1000"));
        assertTrue(variables.get("lineageStartDate").equals("10000"));
        assertTrue(variables.get("filename").equals("fakefile.txt"));
    }

    @Test
    public void testPopulateRegistryWithEmptyFlowFileAndAttributes(){
        System.setProperty("fake","test");
        VariableRegistry variableRegistry = VariableRegistryUtils.createSystemVariableRegistry();
        VariableRegistry populatedRegistry = VariableRegistryUtils.createFlowVariableRegistry(variableRegistry,null,null);
        Map<String,String> variables = populatedRegistry.getVariables();
        assertTrue( variables.containsKey("PATH"));
        assertTrue( variables.get("fake").equals("test"));
    }


    private FlowFile createFlowFile(){
        return  new FlowFile() {
            @Override
            public long getId() {
                return 1;
            }

            @Override
            public long getEntryDate() {
                return 1000;
            }

            @Override
            public long getLineageStartDate() {
                return 10000;
            }

            @Override
            public Long getLastQueueDate() {
                return null;
            }

            @Override
            public boolean isPenalized() {
                return false;
            }

            @Override
            public String getAttribute(String key) {
                return null;
            }

            @Override
            public long getSize() {
                return 50;
            }

            @Override
            public long getLineageStartIndex() {
                return 0;
            }

            @Override
            public long getQueueDateIndex() {
                return 0;
            }

            @Override
            public Map<String, String> getAttributes() {
                Map<String,String> attributes = new HashMap<>();
                attributes.put("filename","fakefile.txt");
                return attributes;
            }

            @Override
            public int compareTo(FlowFile o) {
                return 0;
            }
        };
    }

}
