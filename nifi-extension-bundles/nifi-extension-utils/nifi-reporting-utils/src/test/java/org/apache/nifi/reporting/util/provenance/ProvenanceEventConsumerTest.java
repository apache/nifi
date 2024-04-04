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
package org.apache.nifi.reporting.util.provenance;

import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProvenanceEventConsumerTest {
    private static final String EMPTY = "";

    private ProvenanceEventConsumer consumer;

    @BeforeEach
    public void setConsumer() {
        consumer = new ProvenanceEventConsumer();
    }

    @Test
    public void testDefaultFilteringDisabled() {
        assertFalse(consumer.isFilteringEnabled());
    }

    @Test
    public void testComponentNameRegexEmptyFilteringDisabled() {
        consumer.setComponentNameRegex(EMPTY);

        assertFalse(consumer.isFilteringEnabled());
    }

    @Test
    public void testComponentNameRegexExcludeEmptyFilteringDisabled() {
        consumer.setComponentNameRegexExclude(EMPTY);

        assertFalse(consumer.isFilteringEnabled());
    }

    @Test
    public void testComponentNameRegexFilteringEnabled() {
        consumer.setComponentNameRegex(String.class.getSimpleName());

        assertTrue(consumer.isFilteringEnabled());
    }

    @Test
    public void testComponentNameRegexExcludeFilteringEnabled() {
        consumer.setComponentNameRegexExclude(String.class.getSimpleName());

        assertTrue(consumer.isFilteringEnabled());
    }

    @Test
    public void testComponentTypeRegexFilteringEnabled() {
        consumer.setComponentTypeRegex(String.class.getSimpleName());

        assertTrue(consumer.isFilteringEnabled());
    }

    @Test
    public void testComponentTypeRegexExcludeFilteringEnabled() {
        consumer.setComponentTypeRegexExclude(String.class.getSimpleName());

        assertTrue(consumer.isFilteringEnabled());
    }

    @Test
    public void testComponentTypeRegexEmptyFilteringDisabled() {
        consumer.setComponentTypeRegex(EMPTY);

        assertFalse(consumer.isFilteringEnabled());
    }

    @Test
    public void testComponentTypeRegexExcludeEmptyFilteringDisabled() {
        consumer.setComponentTypeRegexExclude(EMPTY);

        assertFalse(consumer.isFilteringEnabled());
    }

    @Test
    public void testTargetEventTypeFilteringEnabled() {
        consumer.addTargetEventType(ProvenanceEventType.CREATE);

        assertTrue(consumer.isFilteringEnabled());
    }

    @Test
    public void testTargetEventTypeExcludeFilteringEnabled() {
        consumer.addTargetEventTypeExclude(ProvenanceEventType.CREATE);

        assertTrue(consumer.isFilteringEnabled());
    }

    @Test
    public void testTargetComponentIdFilteringEnabled() {
        consumer.addTargetComponentId(String.class.getSimpleName());

        assertTrue(consumer.isFilteringEnabled());
    }

    @Test
    public void testTargetComponentIdExcludeFilteringEnabled() {
        consumer.addTargetComponentIdExclude(String.class.getSimpleName());

        assertTrue(consumer.isFilteringEnabled());
    }
}
