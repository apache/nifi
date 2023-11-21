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

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.queryrecord.RecordDataSource;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.sql.NiFiTable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class SqlJoinStrategy implements RecordJoinStrategy {
    public static final String ENRICHMENT_TABLE_NAME = "ENRICHMENT";
    public static final String ORIGINAL_TABLE_NAME = "ORIGINAL";

    private final SqlJoinCache cache;
    private final ComponentLog logger;
    private final PropertyValue sqlPropertyValue;
    private final int defaultPrecision;
    private final int defaultScale;

    public SqlJoinStrategy(final SqlJoinCache cache, final PropertyValue sqlPropertyValue, final ComponentLog logger, final int defaultPrecision, final int defaultScale) {
        this.cache = cache;
        this.sqlPropertyValue = sqlPropertyValue;
        this.logger = logger;
        this.defaultPrecision = defaultPrecision;
        this.defaultScale = defaultScale;
    }

    @Override
    public RecordJoinResult join(final RecordJoinInput originalInput, final RecordJoinInput enrichmentInput, final Map<String, String> combinedAttributes,
                final ProcessSession session, final RecordSchema outputSchema) throws SQLException {

        final String sql = sqlPropertyValue.evaluateAttributeExpressions(combinedAttributes).getValue();
        final SqlJoinCalciteParameters calciteParameters = cache.getCalciteParameters(sql, outputSchema, originalInput, enrichmentInput);

        final NiFiTable originalTable = calciteParameters.getDatabase().getTable(ORIGINAL_TABLE_NAME);
        originalTable.setDataSource(new RecordDataSource(originalInput.getRecordSchema(), session, originalInput.getFlowFile(), originalInput.getRecordReaderFactory(), logger));

        final NiFiTable enrichmentTable = calciteParameters.getDatabase().getTable(ENRICHMENT_TABLE_NAME);
        enrichmentTable.setDataSource(new RecordDataSource(enrichmentInput.getRecordSchema(), session, enrichmentInput.getFlowFile(), enrichmentInput.getRecordReaderFactory(), logger));

        final PreparedStatement stmt = calciteParameters.getPreparedStatement();

        final ResultSet rs;
        try {
            rs = stmt.executeQuery();
        } catch (final Throwable t) {
            originalTable.close();
            enrichmentTable.close();

            throw t;
        }

        final RecordSet recordSet = new ResultSetRecordSet(rs, outputSchema, defaultPrecision, defaultScale, true);

        // Create a RecordJoinResult that allows us to return our RecordSet and also close all resources when they are no longer needed
        return new RecordJoinResult() {
            @Override
            public void close() {
                closeQuietly(originalTable, enrichmentTable);
                cache.returnCalciteParameters(sql, outputSchema, calciteParameters);
            }

            @Override
            public RecordSet getRecordSet() {
                return recordSet;
            }
        };
    }

    private void closeQuietly(final AutoCloseable... closeables) {
        for (final AutoCloseable closeable : closeables) {
            closeQuietly(closeable);
        }
    }

    private void closeQuietly(final AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Exception e) {
                logger.warn("Failed to close {}", closeable, e);
            }
        }
    }

}
