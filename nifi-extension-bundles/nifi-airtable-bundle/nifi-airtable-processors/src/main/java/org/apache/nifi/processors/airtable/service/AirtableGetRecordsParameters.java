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

package org.apache.nifi.processors.airtable.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

public class AirtableGetRecordsParameters {

    private final List<String> fields;
    private final Optional<String> modifiedAfter;
    private final Optional<String> modifiedBefore;
    private final Optional<String> customFilter;
    private final Optional<String> offset;
    private final OptionalInt pageSize;

    public AirtableGetRecordsParameters(final List<String> fields,
            final Optional<String> modifiedAfter,
            final Optional<String> modifiedBefore,
            final Optional<String> customFilter,
            final Optional<String> offset,
            final OptionalInt pageSize) {
        this.fields = Objects.requireNonNull(fields);
        this.modifiedAfter = modifiedAfter;
        this.modifiedBefore = modifiedBefore;
        this.customFilter = customFilter;
        this.offset = offset;
        this.pageSize = pageSize;
    }

    public List<String> getFields() {
        return fields;
    }

    public Optional<String> getModifiedAfter() {
        return modifiedAfter;
    }

    public Optional<String> getModifiedBefore() {
        return modifiedBefore;
    }

    public Optional<String> getCustomFilter() {
        return customFilter;
    }

    public Optional<String> getOffset() {
        return offset;
    }

    public OptionalInt getPageSize() {
        return pageSize;
    }

    public AirtableGetRecordsParameters withOffset(final String offset) {
        return new AirtableGetRecordsParameters(fields, modifiedAfter, modifiedBefore, customFilter, Optional.of(offset), pageSize);
    }

    public static class Builder {
        private List<String> fields;
        private String modifiedAfter;
        private String modifiedBefore;
        private String customFilter;
        private String offset;
        private OptionalInt pageSize = OptionalInt.empty();

        public Builder fields(final List<String> fields) {
            this.fields = fields;
            return this;
        }

        public Builder field(final String field) {
            if (fields == null) {
                fields = new ArrayList<>();
            }
            fields.add(field);
            return this;
        }

        public Builder modifiedAfter(final String modifiedAfter) {
            this.modifiedAfter = modifiedAfter;
            return this;
        }

        public Builder modifiedBefore(final String modifiedBefore) {
            this.modifiedBefore = modifiedBefore;
            return this;
        }

        public Builder customFilter(final String customFilter) {
            this.customFilter = customFilter;
            return this;
        }

        public Builder offset(final String offset) {
            this.offset = offset;
            return this;
        }

        public Builder pageSize(final int pageSize) {
            this.pageSize = OptionalInt.of(pageSize);
            return this;
        }

        public AirtableGetRecordsParameters build() {
            return new AirtableGetRecordsParameters(fields != null ? fields : new ArrayList<>(),
                    Optional.ofNullable(modifiedAfter),
                    Optional.ofNullable(modifiedBefore),
                    Optional.ofNullable(customFilter),
                    Optional.ofNullable(offset),
                    pageSize);
        }
    }
}
