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
package org.apache.nifi.registry.service;

import org.apache.nifi.registry.params.SortOrder;
import org.apache.nifi.registry.params.SortParameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Parameters to be passed into service layer for methods that require sorting and paging.
 */
public class QueryParameters {

    public static final QueryParameters EMPTY_PARAMETERS = new QueryParameters.Builder().build();

    private final Integer pageNum;

    private final Integer numRows;

    private final List<SortParameter> sortParameters;

    private QueryParameters(final Builder builder) {
        this.pageNum = builder.pageNum;
        this.numRows = builder.numRows;
        this.sortParameters = Collections.unmodifiableList(new ArrayList<>(builder.sortParameters));

        if (this.pageNum != null && this.numRows != null) {
            if (this.pageNum < 0) {
                throw new IllegalStateException("Offset cannot be negative");
            }

            if (this.numRows < 0) {
                throw new IllegalStateException("Number of rows cannot be negative");
            }
        }
    }

    public Integer getPageNum() {
        return pageNum;
    }

    public Integer getNumRows() {
        return numRows;
    }

    public List<SortParameter> getSortParameters() {
        return sortParameters;
    }

    /**
     * Builder for QueryParameters.
     */
    public static class Builder {

        private Integer pageNum;
        private Integer numRows;
        private List<SortParameter> sortParameters = new ArrayList<>();

        public Builder pageNum(Integer pageNum) {
            this.pageNum = pageNum;
            return this;
        }

        public Builder numRows(Integer numRows) {
            this.numRows = numRows;
            return this;
        }

        public Builder addSort(final SortParameter sort) {
            this.sortParameters.add(sort);
            return this;
        }

        public Builder addSort(final String fieldName, final SortOrder order) {
            this.sortParameters.add(new SortParameter(fieldName, order));
            return this;
        }

        public Builder addSorts(final Collection<SortParameter> sorts) {
            if (sorts != null) {
                this.sortParameters.addAll(sorts);
            }
            return this;
        }

        public Builder clearSorts() {
            this.sortParameters.clear();
            return this;
        }

        public QueryParameters build() {
            return new QueryParameters(this);
        }
    }

}
