/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestElasticSearchProcessorMigration {
    private static final Map<String, String> ABSTRACT_PAGINATED_JSON_QUERY_ELASTICSEARCH_MIGRATIONS = Map.of(
            "el-rest-pagination-type", AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE.getName(),
            "el-rest-pagination-keep-alive", AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE.getName()
    );

    private static final Map<String, String> ABSTRACT_JSON_QUERY_ELASTICSEARCH_MIGRATIONS = Map.of(
            "el-rest-split-up-hits", AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT.getName(),
            "el-rest-format-hits", AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT.getName(),
            "el-rest-split-up-aggregations", AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT.getName(),
            "el-rest-format-aggregations", AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT.getName(),
            "el-rest-output-no-hits", AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS.getName()
    );

    private static final Map<String, String> ABSTRACT_PUT_ELASTICSEARCH_MIGRATIONS = Map.of(
            "put-es-output-error-responses", AbstractPutElasticsearch.OUTPUT_ERROR_RESPONSES.getName(),
            "put-es-record-batch-size", AbstractPutElasticsearch.BATCH_SIZE.getName(),
            "put-es-record-index-op", AbstractPutElasticsearch.INDEX_OP.getName(),
            "put-es-not_found-is-error", AbstractPutElasticsearch.NOT_FOUND_IS_SUCCESSFUL.getName()
    );

    private static final Map<String, String> ELASTICSEARCH_REST_PROCESSOR_MIGRATIONS = Map.ofEntries(
            Map.entry("el-rest-fetch-index", ElasticsearchRestProcessor.INDEX.getName()),
            Map.entry("el-rest-type", ElasticsearchRestProcessor.TYPE.getName()),
            Map.entry("el-rest-query-definition-style", ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE.getName()),
            Map.entry("el-rest-query", ElasticsearchRestProcessor.QUERY.getName()),
            Map.entry("el-rest-query-clause", ElasticsearchRestProcessor.QUERY_CLAUSE.getName()),
            Map.entry("el-rest-script", ElasticsearchRestProcessor.SCRIPT.getName()),
            Map.entry("es-rest-size", ElasticsearchRestProcessor.SIZE.getName()),
            Map.entry("es-rest-query-aggs", ElasticsearchRestProcessor.AGGREGATIONS.getName()),
            Map.entry("es-rest-query-sort", ElasticsearchRestProcessor.SORT.getName()),
            Map.entry("es-rest-query-fields", ElasticsearchRestProcessor.FIELDS.getName()),
            Map.entry("es-rest-query-script-fields", ElasticsearchRestProcessor.SCRIPT_FIELDS.getName()),
            Map.entry("el-query-attribute", ElasticsearchRestProcessor.QUERY_ATTRIBUTE.getName()),
            Map.entry("el-rest-client-service", ElasticsearchRestProcessor.CLIENT_SERVICE.getName()),
            Map.entry("put-es-record-log-error-responses", ElasticsearchRestProcessor.LOG_ERROR_RESPONSES.getName())
    );

    @Test
    void testGetElasticsearch() {
        final TestRunner runner = TestRunners.newTestRunner(GetElasticsearch.class);
        final Map<String, String> expectedOwnRenamed = Map.of(
                "get-es-id", GetElasticsearch.ID.getName(),
                "get-es-destination", GetElasticsearch.DESTINATION.getName(),
                "get-es-attribute-name", GetElasticsearch.ATTRIBUTE_NAME.getName()
        );

        final Map<String, String> expectedRenamed = new HashMap<>(expectedOwnRenamed);
        expectedRenamed.putAll(ELASTICSEARCH_REST_PROCESSOR_MIGRATIONS);

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
        assertEquals(Set.of(), propertyMigrationResult.getPropertiesRemoved());
    }

    @Test
    void testSearchElasticsearch() {
        final TestRunner runner = TestRunners.newTestRunner(SearchElasticsearch.class);
        final Map<String, String> expectedOwnRenamed = Map.of(
                "restart-on-finish", SearchElasticsearch.RESTART_ON_FINISH.getName()
        );

        final Map<String, String> expectedRenamed = new HashMap<>(expectedOwnRenamed);
        expectedRenamed.putAll(ABSTRACT_PAGINATED_JSON_QUERY_ELASTICSEARCH_MIGRATIONS);
        expectedRenamed.putAll(ABSTRACT_JSON_QUERY_ELASTICSEARCH_MIGRATIONS);
        expectedRenamed.putAll(ELASTICSEARCH_REST_PROCESSOR_MIGRATIONS);

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
        assertEquals(Set.of(), propertyMigrationResult.getPropertiesRemoved());
    }

    @Test
    void testConsumeElasticsearch() {
        final TestRunner runner = TestRunners.newTestRunner(ConsumeElasticsearch.class);
        final Map<String, String> expectedOwnRenamed = Map.of(
                "es-rest-range-field", ConsumeElasticsearch.RANGE_FIELD.getName(),
                "es-rest-sort-order", ConsumeElasticsearch.RANGE_FIELD_SORT_ORDER.getName(),
                "es-rest-range-initial-value", ConsumeElasticsearch.RANGE_INITIAL_VALUE.getName(),
                "es-rest-range-format", ConsumeElasticsearch.RANGE_DATE_FORMAT.getName(),
                "es-rest-range-time-zone", ConsumeElasticsearch.RANGE_TIME_ZONE.getName(),
                "es-rest-additional-filters", ConsumeElasticsearch.ADDITIONAL_FILTERS.getName(),
                "restart-on-finish", SearchElasticsearch.RESTART_ON_FINISH.getName()
        );

        final Map<String, String> expectedRenamed = new HashMap<>(expectedOwnRenamed);
        expectedRenamed.putAll(ABSTRACT_PAGINATED_JSON_QUERY_ELASTICSEARCH_MIGRATIONS);
        expectedRenamed.putAll(ABSTRACT_JSON_QUERY_ELASTICSEARCH_MIGRATIONS);
        expectedRenamed.putAll(ELASTICSEARCH_REST_PROCESSOR_MIGRATIONS);

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
        assertEquals(Set.of(), propertyMigrationResult.getPropertiesRemoved());
    }

    @Test
    void testPutElasticsearchJson() {
        final TestRunner runner = TestRunners.newTestRunner(PutElasticsearchJson.class);
        final Map<String, String> expectedOwnRenamed = Map.of(
                "put-es-json-id-attr", PutElasticsearchJson.ID_ATTRIBUTE.getName(),
                "put-es-json-script", PutElasticsearchJson.SCRIPT.getName(),
                "put-es-json-scripted-upsert", PutElasticsearchJson.SCRIPTED_UPSERT.getName(),
                "put-es-json-dynamic_templates", PutElasticsearchJson.DYNAMIC_TEMPLATES.getName(),
                "put-es-json-charset", PutElasticsearchJson.CHARSET.getName(),
                "put-es-json-not_found-is-error", AbstractPutElasticsearch.NOT_FOUND_IS_SUCCESSFUL.getName()
        );

        final Map<String, String> expectedRenamed = new HashMap<>(expectedOwnRenamed);
        expectedRenamed.putAll(ABSTRACT_PUT_ELASTICSEARCH_MIGRATIONS);
        expectedRenamed.putAll(ELASTICSEARCH_REST_PROCESSOR_MIGRATIONS);

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());

        final Set<String> expectedRemoved = Set.of("put-es-json-error-documents");
        assertEquals(expectedRemoved, propertyMigrationResult.getPropertiesRemoved());
    }

    @Test
    void testPutElasticsearchRecord() {
        final TestRunner runner = TestRunners.newTestRunner(PutElasticsearchRecord.class);
        final Map<String, String> expectedOwnRenamed = Map.ofEntries(
                Map.entry("put-es-record-reader", PutElasticsearchRecord.RECORD_READER.getName()),
                Map.entry("put-es-record-at-timestamp", PutElasticsearchRecord.AT_TIMESTAMP.getName()),
                Map.entry("put-es-record-index-op-path", PutElasticsearchRecord.INDEX_OP_RECORD_PATH.getName()),
                Map.entry("put-es-record-id-path", PutElasticsearchRecord.ID_RECORD_PATH.getName()),
                Map.entry("put-es-record-retain-id-field", PutElasticsearchRecord.RETAIN_ID_FIELD.getName()),
                Map.entry("Retain ID (Record Path)", PutElasticsearchRecord.RETAIN_ID_FIELD.getName()),
                Map.entry("put-es-record-index-record-path", PutElasticsearchRecord.INDEX_RECORD_PATH.getName()),
                Map.entry("put-es-record-type-record-path", PutElasticsearchRecord.TYPE_RECORD_PATH.getName()),
                Map.entry("put-es-record-at-timestamp-path", PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH.getName()),
                Map.entry("put-es-record-script-path", PutElasticsearchRecord.SCRIPT_RECORD_PATH.getName()),
                Map.entry("put-es-record-scripted-upsert-path", PutElasticsearchRecord.SCRIPTED_UPSERT_RECORD_PATH.getName()),
                Map.entry("put-es-record-dynamic-templates-path", PutElasticsearchRecord.DYNAMIC_TEMPLATES_RECORD_PATH.getName()),
                Map.entry("put-es-record-retain-at-timestamp-field", PutElasticsearchRecord.RETAIN_AT_TIMESTAMP_FIELD.getName()),
                Map.entry("put-es-record-error-writer", PutElasticsearchRecord.RESULT_RECORD_WRITER.getName()),
                Map.entry("put-es-record-bulk-error-groups", PutElasticsearchRecord.GROUP_BULK_ERRORS_BY_TYPE.getName()),
                Map.entry("put-es-record-at-timestamp-date-format", PutElasticsearchRecord.DATE_FORMAT.getName()),
                Map.entry("put-es-record-at-timestamp-time-format", PutElasticsearchRecord.TIME_FORMAT.getName()),
                Map.entry("put-es-record-at-timestamp-timestamp-format", PutElasticsearchRecord.TIMESTAMP_FORMAT.getName()),
                Map.entry("put-es-record-not_found-is-error", AbstractPutElasticsearch.NOT_FOUND_IS_SUCCESSFUL.getName())
        );

        final Map<String, String> expectedRenamed = new HashMap<>(expectedOwnRenamed);
        expectedRenamed.putAll(ABSTRACT_PUT_ELASTICSEARCH_MIGRATIONS);
        expectedRenamed.putAll(ELASTICSEARCH_REST_PROCESSOR_MIGRATIONS);
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
        assertEquals(Set.of(), propertyMigrationResult.getPropertiesRemoved());
        assertEquals(Set.of(PutElasticsearchRecord.RESULT_RECORD_WRITER.getName()), propertyMigrationResult.getPropertiesUpdated());
    }
}
