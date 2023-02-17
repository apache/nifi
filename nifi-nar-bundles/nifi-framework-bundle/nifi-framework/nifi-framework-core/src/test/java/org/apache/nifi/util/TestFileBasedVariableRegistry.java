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
package org.apache.nifi.util;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestFileBasedVariableRegistry {

    @Test
    public void testCreateCustomVariableRegistry() {
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        Path[] paths = {fooPath, testPath};
        final String vendorUrl = System.getProperty("java.vendor.url");
        VariableRegistry variableRegistry = new FileBasedVariableRegistry(paths);
        final Map<VariableDescriptor, String> variables = variableRegistry.getVariableMap();
        assertTrue(variables.containsKey(new VariableDescriptor("fake.property.3")));
        assertEquals(vendorUrl, variableRegistry.getVariableValue("java.vendor.url"));
        assertEquals("test me out 3, test me out 4", variableRegistry.getVariableValue("fake.property.3"));
    }
}
