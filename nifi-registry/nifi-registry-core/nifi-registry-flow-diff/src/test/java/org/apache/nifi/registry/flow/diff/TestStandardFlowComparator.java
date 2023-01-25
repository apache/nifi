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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestStandardFlowComparator {
    private Map<String, String> decryptedToEncrypted;
    private Map<String, String> encryptedToDecrypted;
    private StandardFlowComparator comparator;

    @BeforeEach
    public void setup() {
        decryptedToEncrypted = new HashMap<>();
        decryptedToEncrypted.put("XYZ", "Hello");
        decryptedToEncrypted.put("xyz", "hola");

        encryptedToDecrypted = new HashMap<>();
        encryptedToDecrypted.put("Hello", "XYZ");
        encryptedToDecrypted.put("hola", "xyz");

        final Function<String, String> decryptor = encryptedToDecrypted::get;
        final ComparableDataFlow flowA = new StandardComparableDataFlow("Flow A", new VersionedProcessGroup());
        final ComparableDataFlow flowB = new StandardComparableDataFlow("Flow B", new VersionedProcessGroup());
        comparator = new StandardFlowComparator(flowA, flowB, Collections.emptySet(),
            new StaticDifferenceDescriptor(), decryptor, VersionedComponent::getInstanceIdentifier, FlowComparatorVersionedStrategy.SHALLOW);
    }

    // Ensure that when we are comparing parameter values that we compare the decrypted values, but we don't include any
    // decrypted values in the descriptions of the Flow Difference.
    @Test
    public void testSensitiveParametersDecryptedBeforeCompare() {
        final Set<FlowDifference> differences = new HashSet<>();

        final Set<VersionedParameter> parametersA = new HashSet<>();
        parametersA.add(createParameter("Param 1", "xyz", false));
        parametersA.add(createParameter("Param 2", "XYZ", false));
        parametersA.add(createParameter("Param 3", "Hi there", false));
        parametersA.add(createParameter("Param 4", "xyz", true));
        parametersA.add(createParameter("Param 5", "XYZ", true));

        // Now that we've created the parameters, change the mapping of decrypted to encrypted so that we encrypt the values
        // differently in each context but have the same decrypted value
        decryptedToEncrypted.put("xyz", "bonjour");
        encryptedToDecrypted.put("bonjour", "xyz");

        final Set<VersionedParameter> parametersB = new HashSet<>();
        parametersB.add(createParameter("Param 1", "xyz", false));
        parametersB.add(createParameter("Param 2", "XYZ", false));
        parametersB.add(createParameter("Param 3", "Hey", false));
        parametersB.add(createParameter("Param 4", "xyz", true));
        parametersB.add(createParameter("Param 5", "xyz", true));

        final VersionedParameterContext contextA = new VersionedParameterContext();
        contextA.setIdentifier("id");
        contextA.setInstanceIdentifier("instanceId");
        contextA.setParameters(parametersA);

        final VersionedParameterContext contextB = new VersionedParameterContext();
        contextB.setIdentifier("id");
        contextB.setInstanceIdentifier("instanceId");
        contextB.setParameters(parametersB);

        comparator.compare(contextA, contextB, differences);

        assertEquals(2, differences.size());
        for (final FlowDifference difference : differences) {
            assertSame(DifferenceType.PARAMETER_VALUE_CHANGED, difference.getDifferenceType());

            // Ensure that the sensitive values are not contained in the description
            assertFalse(difference.getDescription().contains("Hello"));
            assertFalse(difference.getDescription().contains("Hola"));
            assertFalse(difference.getDescription().contains("bonjour"));
        }

        final long numContainingValue = differences.stream()
            .filter(diff -> diff.getDescription().contains("Hey") && diff.getDescription().contains("Hi there"))
            .count();
        assertEquals(1, numContainingValue);
    }

    private VersionedParameter createParameter(final String name, final String value, final boolean sensitive) {
        final VersionedParameter parameter = new VersionedParameter();
        parameter.setName(name);
        parameter.setValue(sensitive ? "enc{" + decryptedToEncrypted.get(value) + "}" : value);
        parameter.setSensitive(sensitive);
        return parameter;
    }
}
