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
package org.apache.nifi.processors.script;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestScriptingComponentHelper {

    private static final String SCRIPT_LANGUAGE = "Script Language";
    private static final String SCRIPT_ENGINE = "Script Engine";
    private static final String CONFIGURED_SCRIPT_ENGINE = "AWK";
    private static final String GROOVY_SCRIPT_ENGINE = "Groovy";

    @Test
    void testScriptEngineAllowableValuesWithDescriptions() {
        final ScriptingComponentHelper helper = new ScriptingComponentHelper();
        helper.createResources();

        final List<PropertyDescriptor> descriptors = helper.getDescriptors();
        final Optional<PropertyDescriptor> optionalScriptEngine = descriptors.stream().filter(
                descriptor -> descriptor.getName().equals(
                        ScriptingComponentHelper.getScriptEnginePropertyBuilder().build().getName()
                )
        ).findFirst();

        assertTrue(optionalScriptEngine.isPresent());
        final PropertyDescriptor scriptEngineDescriptor = optionalScriptEngine.get();

        final List<AllowableValue> allowableValues = scriptEngineDescriptor.getAllowableValues();
        assertFalse(allowableValues.isEmpty());

        for (final AllowableValue allowableValue : allowableValues) {
            assertNotNull(allowableValue.getDescription());
        }
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> propertyValues = Map.of(
                SCRIPT_LANGUAGE, CONFIGURED_SCRIPT_ENGINE
        );
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);

        ScriptingComponentHelper.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        final String scriptLanguageRenamed = propertiesRenamed.get(SCRIPT_LANGUAGE);
        assertEquals(SCRIPT_ENGINE, scriptLanguageRenamed);
    }

    @Test
    void testMigratePropertiesPreferringScriptEngineProperty() {
        final Map<String, String> propertyValues = Map.of(
                SCRIPT_LANGUAGE, CONFIGURED_SCRIPT_ENGINE,
                SCRIPT_ENGINE, GROOVY_SCRIPT_ENGINE
        );
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);

        ScriptingComponentHelper.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();

        final Set<String> propertiesRemoved = result.getPropertiesRemoved();
        assertTrue(propertiesRemoved.contains(SCRIPT_LANGUAGE), "Script Language property should be removed");

        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();
        assertTrue(propertiesRenamed.isEmpty(), "Properties should not be renamed when Script Engine is defined");
    }
}
