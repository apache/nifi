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

package org.apache.nifi.elasticsearch;

import java.util.Arrays;
import java.util.Map;

/**
 * A POJO that represents an "operation on an index". It should not be confused with just indexing documents, as it
 * covers all CRUD-related operations that can be executed against an Elasticsearch index with documents.
 * Type is optional and  will not be used in future versions of Elasticsearch.
 */
public class IndexOperationRequest {
    private final String index;
    private final String type;
    private final String id;
    private final Map<String, Object> fields;
    private final Operation operation;
    private final Map<String, Object> script;

    private final boolean scriptedUpsert;
    private final Map<String, Object> dynamicTemplates;
    private final Map<String, String> headerFields;

    public IndexOperationRequest(final String index, final String type, final String id, final Map<String, Object> fields,
                                 final Operation operation, final Map<String, Object> script, final boolean scriptedUpsert,
                                 final Map<String, Object> dynamicTemplates, final Map<String, String> headerFields) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.fields = fields;
        this.operation = operation;
        this.script = script;
        this.scriptedUpsert = scriptedUpsert;
        this.dynamicTemplates = dynamicTemplates;
        this.headerFields = headerFields;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public Operation getOperation() {
        return operation;
    }

    public Map<String, Object> getScript() {
        return script;
    }

    public boolean isScriptedUpsert() {
        return scriptedUpsert;
    }

    public Map<String, Object> getDynamicTemplates() {
        return dynamicTemplates;
    }

    public Map<String, String> getHeaderFields() {
        return headerFields;
    }

    public enum Operation {
        Create("create"),
        Delete("delete"),
        Index("index"),
        Update("update"),
        Upsert("upsert");
        private final String value;

        Operation(final String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Operation forValue(final String value) {
            return Arrays.stream(Operation.values())
                    .filter(o -> o.getValue().equalsIgnoreCase(value)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Unknown Index Operation %s", value)));
        }
    }

    @Override
    public String toString() {
        return "IndexOperationRequest{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", fields=" + fields +
                ", operation=" + operation +
                ", script=" + script +
                ", scriptedUpsert=" + scriptedUpsert +
                ", dynamicTemplates=" + dynamicTemplates +
                ", headerFields=" + headerFields +
                '}';
    }
}