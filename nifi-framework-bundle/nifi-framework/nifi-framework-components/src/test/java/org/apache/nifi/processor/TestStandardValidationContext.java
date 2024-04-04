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

package org.apache.nifi.processor;

import org.apache.nifi.attribute.expression.language.VariableImpact;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.parameter.StandardParameterTokenList;
import org.apache.nifi.processor.util.StandardValidators;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TestStandardValidationContext {

    @Test
    public void testPropertyDependencySatisfied() {
        final ControllerServiceProvider csProvider = mock(ControllerServiceProvider.class);

        final PropertyDescriptor descriptorA = new PropertyDescriptor.Builder()
            .name("A")
            .allowableValues("abc", "xyz")
            .defaultValue("abc")
            .build();

        final PropertyDescriptor descriptorB = new PropertyDescriptor.Builder()
            .name("B")
            .required(true)
            .dependsOn(descriptorA, "abc")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
        final PropertyConfiguration configurationB = null;

        final Function<String, PropertyDescriptor> propertyLookup = propName -> propName.equals("A") ? descriptorA : null;

        final Map<PropertyDescriptor, PropertyConfiguration> properties = new HashMap<>();
        properties.put(descriptorB, configurationB);

        StandardValidationContext context = new StandardValidationContext(csProvider, properties, null, "1234", "12345", null, false);

        // Property B's dependency should be satisfied because A = "abc"
        assertTrue(context.isDependencySatisfied(descriptorB, propertyLookup));

        // Property A's dependency is always satisfied b/c no dependency
        assertTrue(context.isDependencySatisfied(descriptorA, propertyLookup));

        properties.put(descriptorA, new PropertyConfiguration("xyz", new StandardParameterTokenList("xyz", Collections.emptyList()), Collections.emptyList(), VariableImpact.NEVER_IMPACTED));
        context = new StandardValidationContext(csProvider, properties, null, "1234", "12345", null, false);

        // Should not be satisfied because A = "xyz".
        assertFalse(context.isDependencySatisfied(descriptorB, propertyLookup));

        // Property A's dependency should still (always) satisfied b/c no dependency
        assertTrue(context.isDependencySatisfied(descriptorA, propertyLookup));
    }
}
