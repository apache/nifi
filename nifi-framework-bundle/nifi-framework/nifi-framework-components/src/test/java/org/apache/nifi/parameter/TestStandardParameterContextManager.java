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
package org.apache.nifi.parameter;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardParameterContextManager {

    @Test
    public void testNameMappingUpdatedOnAddRemoveAndRename() {
        final StandardParameterContextManager manager = new StandardParameterContextManager();
        final ParameterContext firstContext = createParameterContext("id-1", "Context 1");

        manager.addParameterContext(firstContext);

        final Map<String, ParameterContext> initialNameMapping = manager.getParameterContextNameMapping();
        assertSame(firstContext, initialNameMapping.get("Context 1"));
        assertThrows(UnsupportedOperationException.class, () -> initialNameMapping.put("Other", firstContext));
        assertSame(initialNameMapping, manager.getParameterContextNameMapping());

        manager.setParameterContextName("id-1", "Renamed");

        final Map<String, ParameterContext> renamedMapping = manager.getParameterContextNameMapping();
        assertFalse(renamedMapping.containsKey("Context 1"));
        assertSame(firstContext, renamedMapping.get("Renamed"));

        final ParameterContext secondContext = createParameterContext("id-2", "Context 2");
        manager.addParameterContext(secondContext);
        assertThrows(IllegalStateException.class, () -> manager.setParameterContextName("id-2", "Renamed"));

        assertSame(firstContext, manager.removeParameterContext("id-1"));
        final Map<String, ParameterContext> removedMapping = manager.getParameterContextNameMapping();
        assertFalse(removedMapping.containsKey("Renamed"));
        assertSame(secondContext, removedMapping.get("Context 2"));
    }

    @Test
    public void testReferenceOnlyParameterContextReplacementRemovesPlaceholderName() {
        final StandardParameterContextManager manager = new StandardParameterContextManager();
        final ReferenceOnlyParameterContext referenceOnlyContext = new ReferenceOnlyParameterContext("id-1");

        manager.addParameterContext(referenceOnlyContext);
        final String placeholderName = referenceOnlyContext.getName();
        assertSame(referenceOnlyContext, manager.getParameterContextNameMapping().get(placeholderName));

        final ParameterContext realContext = createParameterContext("id-1", "Real Context");
        manager.addParameterContext(realContext);

        final Map<String, ParameterContext> nameMapping = manager.getParameterContextNameMapping();
        assertFalse(nameMapping.containsKey(placeholderName));
        assertSame(realContext, nameMapping.get("Real Context"));
        assertSame(realContext, manager.getParameterContext("id-1"));

        assertSame(realContext, manager.removeParameterContext("id-1"));
        assertTrue(manager.getParameterContextNameMapping().isEmpty());
    }

    @Test
    public void testDuplicateNameRejected() {
        final StandardParameterContextManager manager = new StandardParameterContextManager();
        manager.addParameterContext(createParameterContext("id-1", "Context"));

        assertThrows(IllegalStateException.class, () -> manager.addParameterContext(createParameterContext("id-2", "Context")));
    }

    @Test
    public void testDuplicateIdentifierRejected() {
        final StandardParameterContextManager manager = new StandardParameterContextManager();
        manager.addParameterContext(createParameterContext("id-1", "Context 1"));

        assertThrows(IllegalStateException.class, () -> manager.addParameterContext(createParameterContext("id-1", "Context 2")));
    }

    private static ParameterContext createParameterContext(final String id, final String name) {
        return new StandardParameterContext.Builder()
                .id(id)
                .name(name)
                .parameterReferenceManager(ParameterReferenceManager.EMPTY)
                .build();
    }
}
