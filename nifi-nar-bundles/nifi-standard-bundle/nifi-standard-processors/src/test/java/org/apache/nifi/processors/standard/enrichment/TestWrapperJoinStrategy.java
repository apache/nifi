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

package org.apache.nifi.processors.standard.enrichment;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestWrapperJoinStrategy extends TestIndexCorrelatedJoinStrategy {
    private final ComponentLog logger = new MockComponentLog("id", "TestWrapperJoinStrategy");

    @Test
    public void testSimpleJoin() {
        final RecordSchema originalSchema = getOriginalSchema();
        final RecordSchema enrichmentSchema = getEnrichmentSchema();
        final Record originalRecord = createOriginalRecord(555);
        final Record enrichmentRecord = createEnrichmentRecord(555, "John Doe", 100);

        final WrapperJoinStrategy strategy = new WrapperJoinStrategy(logger);

        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);
        final List<RecordField> resultFields = resultSchema.getFields();
        assertEquals(2, resultFields.size());

        final RecordDataType originalDataType = (RecordDataType) resultSchema.getDataType("original").get();
        assertEquals(originalSchema, originalDataType.getChildSchema());

        final RecordDataType enrichmentDataType = (RecordDataType) resultSchema.getDataType("enrichment").get();
        assertEquals(enrichmentSchema, enrichmentDataType.getChildSchema());

        final Record combined = strategy.combineRecords(originalRecord, enrichmentRecord, resultSchema);
        assertEquals(originalRecord, combined.getValue("original"));
        assertEquals(enrichmentRecord, combined.getValue("enrichment"));
    }

    @Test
    public void testNullEnrichment() {
        final RecordSchema originalSchema = getOriginalSchema();
        final Record originalRecord = createOriginalRecord(555);
        final Record enrichmentRecord = null;

        final WrapperJoinStrategy strategy = new WrapperJoinStrategy(logger);

        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);
        final List<RecordField> resultFields = resultSchema.getFields();
        assertEquals(1, resultFields.size());

        final RecordDataType originalDataType = (RecordDataType) resultSchema.getDataType("original").get();
        assertEquals(originalSchema, originalDataType.getChildSchema());

        final Record combined = strategy.combineRecords(originalRecord, enrichmentRecord, resultSchema);
        assertEquals(originalRecord, combined.getValue("original"));
        assertEquals(enrichmentRecord, combined.getValue("enrichment"));
    }

    @Test
    public void testNullOriginal() {
        final RecordSchema enrichmentSchema = getEnrichmentSchema();
        final Record originalRecord = null;
        final Record enrichmentRecord = createEnrichmentRecord(555, "John Doe", 100);

        final WrapperJoinStrategy strategy = new WrapperJoinStrategy(logger);

        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);
        final List<RecordField> resultFields = resultSchema.getFields();
        assertEquals(1, resultFields.size());

        final RecordDataType enrichmentDataType = (RecordDataType) resultSchema.getDataType("enrichment").get();
        assertEquals(enrichmentSchema, enrichmentDataType.getChildSchema());

        final Record combined = strategy.combineRecords(originalRecord, enrichmentRecord, resultSchema);
        assertEquals(originalRecord, combined.getValue("original"));
        assertEquals(enrichmentRecord, combined.getValue("enrichment"));
    }

    @Test
    public void testCombineNullOriginalNullEnrichment() {
        final Record originalRecord = null;
        final Record enrichmentRecord = null;

        final WrapperJoinStrategy strategy = new WrapperJoinStrategy(logger);

        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);
        final List<RecordField> resultFields = resultSchema.getFields();
        assertEquals(0, resultFields.size());

        final Record combined = strategy.combineRecords(originalRecord, enrichmentRecord, resultSchema);
        assertNull(combined);
    }

}
