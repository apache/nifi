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
package org.apache.nifi.migration;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPropertyMigrationPreview {

    @Test
    public void testPreviewMapsObsoleteBooleanToReplacementAndDoesNotMutateFurtherCalls() {
        final ReplacingProcessor processor = new ReplacingProcessor();

        final Optional<Map<String, String>> migratedTrue = PropertyMigrationPreview.preview(
                processor, null, "id", "test", Function.identity(), Map.of(ReplacingProcessor.OBSOLETE, "true"));
        assertTrue(migratedTrue.isPresent());
        assertEquals(ReplacingProcessor.VALIDATE, migratedTrue.get().get(ReplacingProcessor.REPLACEMENT));
        assertFalse(migratedTrue.get().containsKey(ReplacingProcessor.OBSOLETE));

        final Optional<Map<String, String>> migratedFalse = PropertyMigrationPreview.preview(
                processor, null, "id", "test", Function.identity(), Map.of(ReplacingProcessor.OBSOLETE, "false"));
        assertTrue(migratedFalse.isPresent());
        assertEquals(ReplacingProcessor.NONE, migratedFalse.get().get(ReplacingProcessor.REPLACEMENT));

        final Optional<Map<String, String>> alreadyMigrated = PropertyMigrationPreview.preview(
                processor, null, "id", "test", Function.identity(), Map.of(ReplacingProcessor.REPLACEMENT, ReplacingProcessor.VALIDATE));
        assertTrue(alreadyMigrated.isEmpty());
    }

    @Test
    public void testPreviewReturnsEmptyWhenComponentIsNull() {
        assertTrue(PropertyMigrationPreview.preview(null, null, "id", "test", Function.identity(), Map.of("a", "b")).isEmpty());
    }

    private static class ReplacingProcessor extends AbstractProcessor {
        static final String OBSOLETE = "Validate Field Names";
        static final String REPLACEMENT = "Validation Strategy";
        static final String VALIDATE = "VALIDATE";
        static final String NONE = "NONE";

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of(new PropertyDescriptor.Builder().name(REPLACEMENT).defaultValue(VALIDATE).build());
        }

        @Override
        public void migrateProperties(final PropertyConfiguration config) {
            if (config.hasProperty(OBSOLETE) && config.isPropertySet(OBSOLETE)) {
                final boolean validate = Boolean.parseBoolean(config.getRawPropertyValue(OBSOLETE).orElse(Boolean.TRUE.toString()));
                config.setProperty(REPLACEMENT, validate ? VALIDATE : NONE);
            }
            config.removeProperty(OBSOLETE);
        }
    }
}
