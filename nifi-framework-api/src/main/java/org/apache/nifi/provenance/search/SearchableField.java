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
package org.apache.nifi.provenance.search;

/**
 * A SearchableField represents a field in a Provenance Event that can be
 * searched
 */
public interface SearchableField {

    /**
     * @return the identifier that is used to refer to this field
     */
    String getIdentifier();

    /**
     * @return the name of the field that is used when searching the repository
     */
    String getSearchableFieldName();

    /**
     * @return the "friendly" name or "display name" of the field, which may be
     * more human-readable than the searchable field name
     */
    String getFriendlyName();

    /**
     * @return the type of the data stored in this field
     */
    SearchableFieldType getFieldType();

    /**
     * @return <code>true</code> if this field represents a FlowFile attribute,
     * <code>false</code> if the field represents a Provenance Event detail,
     * such as Source System URI
     */
    boolean isAttribute();
}
