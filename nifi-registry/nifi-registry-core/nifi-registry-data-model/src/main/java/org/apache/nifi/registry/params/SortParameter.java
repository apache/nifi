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
package org.apache.nifi.registry.params;

/**
 * Sort parameter made up of a field and a sort order.
 */
public class SortParameter {

    public static final String API_PARAM_DESCRIPTION =
            "Apply client-defined sorting to the resulting list of resource objects. " +
                    "The value of this parameter should be in the format \"field:order\". " +
                    "Valid values for 'field' can be discovered via GET :resourceURI/fields. " +
                    "Valid values for 'order' are 'ASC' (ascending order), 'DESC' (descending order).";

    private final String fieldName;

    private final SortOrder order;

    public SortParameter(final String fieldName, final SortOrder order) {
        this.fieldName = fieldName;
        this.order = order;

        if (this.fieldName == null) {
            throw new IllegalStateException("Field Name cannot be null");
        }

        if (this.fieldName.trim().isEmpty()) {
            throw new IllegalStateException("Field Name cannot be blank");
        }

        if (this.order == null) {
            throw new IllegalStateException("Order cannot be null");
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    public SortOrder getOrder() {
        return order;
    }

    /**
     * Parses a sorting expression of the form field:order.
     *
     * @param sortExpression the expression
     * @return the Sort instance
     */
    public static SortParameter fromString(final String sortExpression) {
        if (sortExpression == null) {
            throw new IllegalArgumentException("Sort cannot be null");
        }

        final String[] sortParts = sortExpression.split("[:]");
        if (sortParts.length != 2) {
            throw new IllegalArgumentException("Sort must be in the form field:order");
        }

        final String fieldName = sortParts[0];
        final SortOrder order = SortOrder.fromString(sortParts[1]);

        return new SortParameter(fieldName, order);
    }

    @Override
    public String toString() {
        return fieldName + ":" + order.getName();
    }
}
