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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestVariableRegistry {

    @Test
    public void testReadMap(){
        Map<String,String> variables1 = new HashMap<>();
        variables1.put("fake.property.1","fake test value");

        Map<String,String> variables2 = new HashMap<>();
        variables1.put("fake.property.2","fake test value");

        VariableRegistry registry = VariableRegistryFactory.getInstance(variables1,variables2);

        Map<String,String> variables = registry.getVariables();
        assertTrue(variables.size() == 2);
        assertTrue(variables.get("fake.property.1").equals("fake test value"));
        assertTrue(registry.getVariableValue("fake.property.2").equals("fake test value"));
    }

    @Test
    public void testReadProperties(){
        Properties properties = new Properties();
        properties.setProperty("fake.property.1","fake test value");
        VariableRegistry registry = VariableRegistryFactory.getPropertiesInstance(properties);
        Map<String,String> variables = registry.getVariables();
        assertTrue(variables.get("fake.property.1").equals("fake test value"));
    }

    @Test
    public void testReadFiles() throws IOException{
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        VariableRegistry registry = VariableRegistryFactory.getPropertiesInstance(fooPath.toFile(),testPath.toFile());
        Map<String,String> variables = registry.getVariables();
        assertTrue(variables.size() == 3);
        assertTrue(variables.get("fake.property.1").equals("test me out 1"));
        assertTrue(variables.get("fake.property.3").equals("test me out 3, test me out 4"));
    }

    @Test
    public void testExcludeInvalidFiles() throws IOException{
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        final Path fakePath = Paths.get("src/test/resources/TestVariableRegistry/fake.properties");
        VariableRegistry registry = VariableRegistryFactory.getPropertiesInstance(fooPath.toFile(),testPath.toFile(),fakePath.toFile());
        Map<String,String> variables = registry.getVariables();
        assertTrue(variables.size() == 3);
        assertTrue(variables.get("fake.property.1").equals("test me out 1"));
        assertTrue(variables.get("fake.property.3").equals("test me out 3, test me out 4"));
    }


    @Test
    public void testReadPaths() throws IOException{
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        VariableRegistry registry = VariableRegistryFactory.getPropertiesInstance(fooPath,testPath);
        Map<String,String> variables = registry.getVariables();
        assertTrue(variables.size() == 3);
        assertTrue(variables.get("fake.property.1").equals("test me out 1"));
        assertTrue(variables.get("fake.property.3").equals("test me out 3, test me out 4"));
    }

    @Test
    public void testExcludeInvalidPaths() throws IOException{
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        final Path fakePath = Paths.get("src/test/resources/TestVariableRegistry/fake.properties");
        VariableRegistry registry = VariableRegistryFactory.getPropertiesInstance(fooPath,testPath,fakePath);
        Map<String,String> variables = registry.getVariables();
        assertTrue(variables.size() == 3);
    }

    @Test
    public void testAddRegistry() throws IOException{

        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        VariableRegistry pathRegistry = VariableRegistryFactory.getPropertiesInstance(fooPath);

        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        VariableRegistry fileRegistry = VariableRegistryFactory.getPropertiesInstance(testPath.toFile());

        Properties properties = new Properties();
        properties.setProperty("fake.property.5","test me out 5");
        VariableRegistry propRegistry = VariableRegistryFactory.getPropertiesInstance(properties);

        propRegistry.addRegistry(pathRegistry);
        propRegistry.addRegistry(fileRegistry);

        Map<String,String> variables = propRegistry.getVariables();
        assertTrue(variables.size() == 4);
    }

    @Test
    public void testAttemptToAddNullRegistry() throws IOException{

        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        VariableRegistry pathRegistry = VariableRegistryFactory.getPropertiesInstance(fooPath);
        VariableRegistry nullRegistry = null;
        pathRegistry.addRegistry(nullRegistry);
        assertTrue(pathRegistry.getVariables().size() == 1);
    }

    @Test
    public void testNoOverwriteRegistry()throws IOException{
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        VariableRegistry pathRegistry = VariableRegistryFactory.getPropertiesInstance(fooPath);

        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        VariableRegistry fileRegistry = VariableRegistryFactory.getPropertiesInstance(testPath.toFile());

        Properties properties = new Properties();
        properties.setProperty("fake.property.3","test me out 5");
        VariableRegistry propRegistry = VariableRegistryFactory.getPropertiesInstance(properties);

        propRegistry.addRegistry(pathRegistry);
        propRegistry.addRegistry(fileRegistry);

        Map<String,String> variables = propRegistry.getVariables();
        String testDuplicate = propRegistry.getVariableValue("fake.property.3");
        assertTrue(variables.size() == 3);
        assertTrue(testDuplicate.equals("test me out 5"));
    }

    @Test
    public void testGetVariableNames() throws IOException{
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        VariableRegistry registry = VariableRegistryFactory.getPropertiesInstance(fooPath,testPath);
        Set<String> variableNames= registry.getVariableNames();
        assertTrue(variableNames.size() == 3);
        assertTrue(variableNames.contains("fake.property.1"));
        assertTrue(variableNames.contains("fake.property.2"));
        assertTrue(variableNames.contains("fake.property.3"));
        assertTrue(!variableNames.contains("fake.property.4"));
    }



}
