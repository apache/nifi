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
package org.apache.nifi.processors.salesforce.util;

import java.util.ArrayList;
import java.util.List;

public class SalesforceQueryBuilder {

    private final IncrementalContext incrementalContext;

    public SalesforceQueryBuilder(final IncrementalContext incrementalContext) {
        this.incrementalContext = incrementalContext;
    }

    public String buildQuery(final String sObject, final String fields, final String customWhereClause) {
        final StringBuilder queryBuilder = new StringBuilder("SELECT ")
                .append(fields)
                .append(" FROM ")
                .append(sObject);

        final List<String> whereItems = new ArrayList<>();
        if (customWhereClause != null) {
            whereItems.add("( " + customWhereClause + " )");
        }

        addAgeFilter(whereItems);

        if (!whereItems.isEmpty()) {
            final String finalWhereClause = String.join(" AND ", whereItems);
            queryBuilder.append(" WHERE ").append(finalWhereClause);
        }

        return queryBuilder.toString();
    }

    private void addAgeFilter(final List<String> whereItems) {
        final String ageField = incrementalContext.getAgeField();
        final String ageFilterLower = incrementalContext.getAgeFilterLower();
        final String initialAgeFilter = incrementalContext.getInitialAgeFilter();

        if (ageField != null) {
            if (ageFilterLower != null) {
                whereItems.add(ageField + " >= " + ageFilterLower);
            } else if (initialAgeFilter != null) {
                whereItems.add(ageField + " >= " + initialAgeFilter);
            }

            whereItems.add(ageField + " < " + incrementalContext.getAgeFilterUpper());
        }
    }
}
