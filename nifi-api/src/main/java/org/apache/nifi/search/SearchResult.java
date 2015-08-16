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
package org.apache.nifi.search;

/**
 *
 */
public class SearchResult {

    private final String label;
    private final String match;

    private SearchResult(final Builder builder) {
        this.label = builder.label;
        this.match = builder.match;
    }

    /**
     * @return the label for this search result
     */
    public String getLabel() {
        return label;
    }

    /**
     * @return the matching string for this search result
     */
    public String getMatch() {
        return match;
    }

    public static final class Builder {

        private String label;
        private String match;

        /**
         * Set the label for the search result.
         *
         * @param label to set
         * @return the builder
         */
        public Builder label(final String label) {
            this.label = label;
            return this;
        }

        /**
         * Set the matching string for the search result.
         *
         * @param match string
         * @return the builder
         */
        public Builder match(final String match) {
            this.match = match;
            return this;
        }

        public SearchResult build() {
            return new SearchResult(this);
        }

    }
}
