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
package org.apache.nifi.provenance;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.provenance.search.SearchableField;

public class SearchableFieldParser {

    public static List<SearchableField> extractSearchableFields(final String indexedFieldString, final boolean predefinedField) {
        final List<SearchableField> searchableFields = new ArrayList<>();
        if (indexedFieldString != null) {
            final String[] split = indexedFieldString.split(",");
            for (String fieldName : split) {
                fieldName = fieldName.trim();
                if (fieldName.isEmpty()) {
                    continue;
                }

                final SearchableField searchableField;
                if (predefinedField) {
                    searchableField = SearchableFields.getSearchableField(fieldName);
                } else {
                    searchableField = SearchableFields.newSearchableAttribute(fieldName);
                }

                if (searchableField == null) {
                    throw new RuntimeException("Invalid Configuration: Provenance Repository configured to Index field '" + fieldName + "', but this is not a valid field");
                }
                searchableFields.add(searchableField);
            }
        }

        return searchableFields;
    }

}
