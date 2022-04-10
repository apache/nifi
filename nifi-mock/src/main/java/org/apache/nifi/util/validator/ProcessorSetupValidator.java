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
package org.apache.nifi.util.validator;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ProcessorSetupValidator extends BaseSetupValidator {
    private static final String PROCESSOR_FILE_PATH = "/META-INF/services/org.apache.nifi.processor.Processor";

    public ProcessorSetupValidator(Processor component) {
        super((AbstractConfigurableComponent) component);
    }

    public ProcessorSetupValidator() {

    }

    @Override
    protected void continueValidation() {
        validateExistanceOfComponentInServices();
        validateRelationships();
    }

    /**
     * Examine the META-INF/services/org.apache.nifi.processor.Processor file to see if this component
     * is registered inside of it.
     */
    public void validateExistanceOfComponentInServices() {
        InputStream processorFile = getClasspathResourceAsInputStream(PROCESSOR_FILE_PATH);
        assertNotNull(processorFile, String.format("%s was missing from the classpath", PROCESSOR_FILE_PATH));
        String contents = readContents(processorFile);
        String[] lines = contents.split("[\\r\\n]");
        String canonicalName = component.getClass().getCanonicalName();
        int found = 0;

        for (String line : lines) {
            if (line.trim().equals(canonicalName)) {
                found++;
            }
        }

        assertTrue(found > 0, String.format("%s was not in %s", canonicalName, PROCESSOR_FILE_PATH));
        assertEquals(1, found, String.format("%s was found multiple times in %s", canonicalName, PROCESSOR_FILE_PATH));
    }

    private String readContents(InputStream is) {
        AtomicReference<String> contents = new AtomicReference<>();
        assertDoesNotThrow(() -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            StringBuilder builder = new StringBuilder();
            String line = reader.readLine();
            while (line != null) {
                builder.append(line).append("\n");
                line = reader.readLine();
            }
            contents.set(builder.toString());
        }, "Exception thrown while reading " + PROCESSOR_FILE_PATH);

        return contents.get();
    }

    public void validateRelationships() {
        Set<Relationship> relationshipsInSet = ((Processor)component).getRelationships();
        assertNotNull(relationshipsInSet, "getRelationships returned a null Set");

        try {
            Arrays.asList(component.getClass().getDeclaredFields())
                .stream()
                .filter(field -> field.getType() == Relationship.class)
                .forEach(field -> {
                    try {
                        Object raw = field.get(component);
                        Relationship relationship = (Relationship)raw;
                        assertTrue(relationshipsInSet.contains(relationship),
                                String.format("Relationship %s not in relationship set", relationship.getName()));
                    } catch (IllegalAccessException e) {
                        fail("Could not retrieve relationship field");
                    }
                });
        } catch (Exception ex) {
            fail("Could not examine relationship fields");
        }
    }
}
