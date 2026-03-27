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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private final byte[] rawJsonBytes;
    private final Operation operation;
    private final Map<String, Object> script;
    private final boolean scriptedUpsert;
    private final Map<String, Object> dynamicTemplates;
    private final Map<String, String> headerFields;

    private IndexOperationRequest(final Builder builder) {
        this.index = builder.index;
        this.type = builder.type;
        this.id = builder.id;
        this.fields = builder.fields;
        this.rawJsonBytes = builder.rawJsonBytes;
        this.operation = builder.operation;
        this.script = builder.script;
        this.scriptedUpsert = builder.scriptedUpsert;
        this.dynamicTemplates = builder.dynamicTemplates;
        this.headerFields = builder.headerFields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String index;
        private String type;
        private String id;
        private Map<String, Object> fields;
        private byte[] rawJsonBytes;
        private Operation operation;
        private Map<String, Object> script;
        private boolean scriptedUpsert;
        private Map<String, Object> dynamicTemplates;
        private Map<String, String> headerFields;

        public Builder index(final String index) {
            this.index = index;
            return this;
        }

        public Builder type(final String type) {
            this.type = type;
            return this;
        }

        public Builder id(final String id) {
            this.id = id;
            return this;
        }

        public Builder fields(final Map<String, Object> fields) {
            this.fields = fields;
            return this;
        }

        public Builder rawJson(final String rawJson) {
            this.rawJsonBytes = rawJson != null ? rawJson.getBytes(StandardCharsets.UTF_8) : null;
            return this;
        }

        public Builder rawJsonBytes(final byte[] rawJsonBytes) {
            this.rawJsonBytes = rawJsonBytes;
            return this;
        }

        public Builder operation(final Operation operation) {
            this.operation = operation;
            return this;
        }

        public Builder script(final Map<String, Object> script) {
            this.script = script;
            return this;
        }

        public Builder scriptedUpsert(final boolean scriptedUpsert) {
            this.scriptedUpsert = scriptedUpsert;
            return this;
        }

        public Builder dynamicTemplates(final Map<String, Object> dynamicTemplates) {
            this.dynamicTemplates = dynamicTemplates;
            return this;
        }

        public Builder headerFields(final Map<String, String> headerFields) {
            this.headerFields = headerFields;
            return this;
        }

        public IndexOperationRequest build() {
            Objects.requireNonNull(index, "Index required");
            Objects.requireNonNull(operation, "Operation required");
            return new IndexOperationRequest(this);
        }
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

    public byte[] getRawJsonBytes() {
        return rawJsonBytes;
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

        private static final List<Operation> VALUES = List.of(Operation.values());

        private final String value;

        Operation(final String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Operation forValue(final String value) {
            for (final Operation operation : VALUES) {
                if (operation.value.equalsIgnoreCase(value)) {
                    return operation;
                }
            }
            throw new IllegalArgumentException(String.format("Unknown Index Operation %s", value));
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
