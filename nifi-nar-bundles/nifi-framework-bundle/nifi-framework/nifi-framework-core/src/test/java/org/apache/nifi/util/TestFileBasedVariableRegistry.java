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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 *
 */
public class TestFileBasedVariableRegistry {

    @Test
    public void testCreateCustomVariableRegistry() {
        final Path fooPath = Paths.get("src/test/resources/TestVariableRegistry/foobar.properties");
        final Path testPath = Paths.get("src/test/resources/TestVariableRegistry/test.properties");
        final Path[] paths = {fooPath, testPath};
        final String vendorUrl = System.getProperty("java.vendor.url");
        final VariableRegistry variableRegistry = new FileBasedVariableRegistry(paths);
        final Map<VariableDescriptor, String> variables = variableRegistry.getVariableMap();

        assertThat(variables, hasEntry(new VariableDescriptor("java.vendor.url"), vendorUrl));
        assertEquals(vendorUrl, variableRegistry.getVariableValue("java.vendor.url"));

        assertThat(variables, hasEntry(new VariableDescriptor("fake.property.3"), "test me out 3, test me out 4"));
        assertEquals("test me out 3, test me out 4", variableRegistry.getVariableValue("fake.property.3"));
    }

    @Test
    public void testCustomVariableRegistryWithCheckSchedule() throws IOException, InterruptedException {
        final Path testPath = Files.createTempFile("test", "properties");
        final File testFile = testPath.toFile();
        try {
            FileOutputStream fos = new FileOutputStream(testFile);
            try {
                fos.write("fake.property.3=test me out 3, test me out 4\n".getBytes("UTF-8"));
                fos.getFD().sync();
            } finally {
                fos.close();
            }

            final Path[] paths = {testPath};
            final VariableRegistry variableRegistry = new FileBasedVariableRegistry(paths, "10 ms");
            Map<VariableDescriptor, String> variables = variableRegistry.getVariableMap();

            assertThat(variables, hasEntry(new VariableDescriptor("fake.property.3"), "test me out 3, test me out 4"));
            assertEquals("test me out 3, test me out 4", variableRegistry.getVariableValue("fake.property.3"));

            fos = new FileOutputStream(testFile);
            try {
                fos.write("fake.property.3=test me out 3\n".getBytes("UTF-8"));
                fos.write("fake.property.4=test me out 4\n".getBytes("UTF-8"));
                fos.getFD().sync();
            } finally {
                fos.close();
            }

            Thread.sleep(100L);

            variables = variableRegistry.getVariableMap();

            assertThat(variables, hasEntry(new VariableDescriptor("fake.property.3"), "test me out 3"));
            assertThat(variables, hasEntry(new VariableDescriptor("fake.property.4"), "test me out 4"));
            assertEquals("test me out 3", variableRegistry.getVariableValue("fake.property.3"));
            assertEquals("test me out 4", variableRegistry.getVariableValue("fake.property.4"));
        } finally {
            Files.deleteIfExists(testPath);
        }
    }

    @Test
    public void testCustomVariableRegistryWithoutCheckSchedule() throws IOException, InterruptedException {
        final Path testPath = Files.createTempFile("test", "properties");
        final File testFile = testPath.toFile();
        try {
            FileOutputStream fos = new FileOutputStream(testFile);
            try {
                fos.write("fake.property.3=test me out 3, test me out 4\n".getBytes("UTF-8"));
                fos.getFD().sync();
            } finally {
                fos.close();
            }

            final Path[] paths = {testPath};
            final VariableRegistry variableRegistry = new FileBasedVariableRegistry(paths, "");
            Map<VariableDescriptor, String> variables = variableRegistry.getVariableMap();

            assertThat(variables, hasEntry(new VariableDescriptor("fake.property.3"), "test me out 3, test me out 4"));
            assertEquals("test me out 3, test me out 4", variableRegistry.getVariableValue("fake.property.3"));

            fos = new FileOutputStream(testFile);
            try {
                fos.write("fake.property.3=test me out 3\n".getBytes("UTF-8"));
                fos.write("fake.property.4=test me out 4\n".getBytes("UTF-8"));
                fos.getFD().sync();
            } finally {
                fos.close();
            }

            Thread.sleep(100L);

            variables = variableRegistry.getVariableMap();

            assertThat(variables, hasEntry(new VariableDescriptor("fake.property.3"), "test me out 3, test me out 4"));
            assertThat(variables, not(hasKey(new VariableDescriptor("fake.property.4"))));
            assertEquals("test me out 3, test me out 4", variableRegistry.getVariableValue("fake.property.3"));
        } finally {
            Files.deleteIfExists(testPath);
        }
    }

    @Test
    public void testCustomVariableRegistryWithZeroCheckSchedule() throws IOException, InterruptedException {
        final Path testPath = Files.createTempFile("test", "properties");
        final File testFile = testPath.toFile();
        try {
            FileOutputStream fos = new FileOutputStream(testFile);
            try {
                fos.write("fake.property.3=test me out 3, test me out 4\n".getBytes("UTF-8"));
                fos.getFD().sync();
            } finally {
                fos.close();
            }

            final Path[] paths = {testPath};
            final VariableRegistry variableRegistry = new FileBasedVariableRegistry(paths, "0 secs");
            Map<VariableDescriptor, String> variables = variableRegistry.getVariableMap();

            assertThat(variables, hasEntry(new VariableDescriptor("fake.property.3"), "test me out 3, test me out 4"));
            assertEquals("test me out 3, test me out 4", variableRegistry.getVariableValue("fake.property.3"));

            fos = new FileOutputStream(testFile);
            try {
                fos.write("fake.property.3=test me out 3\n".getBytes("UTF-8"));
                fos.write("fake.property.4=test me out 4\n".getBytes("UTF-8"));
                fos.getFD().sync();
            } finally {
                fos.close();
            }

            Thread.sleep(100L);

            variables = variableRegistry.getVariableMap();

            assertThat(variables, hasEntry(new VariableDescriptor("fake.property.3"), "test me out 3, test me out 4"));
            assertThat(variables, not(hasKey(new VariableDescriptor("fake.property.4"))));
            assertEquals("test me out 3, test me out 4", variableRegistry.getVariableValue("fake.property.3"));
        } finally {
            Files.deleteIfExists(testPath);
        }
    }

}
