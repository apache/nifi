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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestScriptingComponentHelper {
    private static final String SCRIPT_ENGINE_PROPERTY = "Script Engine";

    @Test
    public void testScriptEngineAllowableValuesWithDescriptions() {
        final ScriptingComponentHelper helper = new ScriptingComponentHelper();
        helper.createResources();

        final List<PropertyDescriptor> descriptors = helper.getDescriptors();
        final Optional<PropertyDescriptor> optionalScriptEngine = descriptors.stream().filter(
                descriptor -> descriptor.getName().equals(SCRIPT_ENGINE_PROPERTY)
        ).findFirst();

        assertTrue(optionalScriptEngine.isPresent());
        final PropertyDescriptor scriptEngineDescriptor = optionalScriptEngine.get();

        final List<AllowableValue> allowableValues =scriptEngineDescriptor.getAllowableValues();
        assertFalse(allowableValues.isEmpty());

        for (final AllowableValue allowableValue : allowableValues) {
            assertNotNull(allowableValue.getDescription());
        }
    }
}
