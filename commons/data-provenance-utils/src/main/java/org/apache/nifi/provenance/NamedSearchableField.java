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

import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.provenance.search.SearchableFieldType;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class NamedSearchableField implements SearchableField {

    private final String identifier;
    private final String searchableName;
    private final SearchableFieldType fieldType;
    private final String friendlyName;
    private final boolean attribute;

    NamedSearchableField(final String identifier, final String searchableName, final String friendlyName, final boolean attribute) {
        this(identifier, searchableName, friendlyName, attribute, SearchableFieldType.STRING);
    }

    NamedSearchableField(final String identifier, final String searchableName, final String friendlyName, final boolean attribute, final SearchableFieldType fieldType) {
        this.identifier = requireNonNull(identifier);
        this.searchableName = requireNonNull(searchableName);
        this.friendlyName = requireNonNull(friendlyName);
        this.attribute = requireNonNull(attribute);
        this.fieldType = requireNonNull(fieldType);
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getSearchableFieldName() {
        return searchableName;
    }

    @Override
    public String getFriendlyName() {
        return friendlyName;
    }

    @Override
    public boolean isAttribute() {
        return attribute;
    }

    @Override
    public SearchableFieldType getFieldType() {
        return fieldType;
    }

    @Override
    public String toString() {
        return friendlyName;
    }

    @Override
    public int hashCode() {
        return 298347 + searchableName.hashCode() + (attribute ? 1 : 0);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof SearchableField)) {
            return false;
        }

        final SearchableField other = (SearchableField) obj;
        return (this.searchableName.equals(other.getSearchableFieldName()) && attribute == other.isAttribute());
    }
}
