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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SearchableFields {

    public static final SearchableField Identifier = new NamedSearchableField("Identifier", "identifier", "Identifier", false);
    public static final SearchableField EventTime = new NamedSearchableField("EventTime", "time", "Event Time", false, SearchableFieldType.DATE);
    public static final SearchableField FlowFileUUID = new NamedSearchableField("FlowFileUUID", "uuid", "FlowFile UUID", false);
    public static final SearchableField Filename = new NamedSearchableField("Filename", "filename", "Filename", false);
    public static final SearchableField EventType = new NamedSearchableField("EventType", "eventType", "Event Type", false);
    public static final SearchableField TransitURI = new NamedSearchableField("TransitURI", "transitUri", "Transit URI", false);
    public static final SearchableField ComponentID = new NamedSearchableField("ProcessorID", "processorId", "Component ID", false);
    public static final SearchableField AlternateIdentifierURI = new NamedSearchableField("AlternateIdentifierURI", "alternateIdentifierUri", "Alternate Identifier URI", false);
    public static final SearchableField FileSize = new NamedSearchableField("FileSize", "fileSize", "File Size", false, SearchableFieldType.DATA_SIZE);
    public static final SearchableField Details = new NamedSearchableField("Details", "details", "Details", false, SearchableFieldType.STRING);
    public static final SearchableField Relationship = new NamedSearchableField("Relationship", "relationship", "Relationship", false, SearchableFieldType.STRING);

    public static final SearchableField LineageStartDate
            = new NamedSearchableField("LineageStartDate", "lineageStartDate", "Lineage Start Date", false, SearchableFieldType.DATE);
    public static final SearchableField LineageIdentifier
            = new NamedSearchableField("LineageIdentifiers", "lineageIdentifier", "Lineage Identifier", false, SearchableFieldType.STRING);

    public static final SearchableField ContentClaimSection
            = new NamedSearchableField("ContentClaimSection", "contentClaimSection", "Content Claim Section", false, SearchableFieldType.STRING);
    public static final SearchableField ContentClaimContainer
            = new NamedSearchableField("ContentClaimContainer", "contentClaimContainer", "Content Claim Container", false, SearchableFieldType.STRING);
    public static final SearchableField ContentClaimIdentifier
            = new NamedSearchableField("ContentClaimIdentifier", "contentClaimIdentifier", "Content Claim Identifier", false, SearchableFieldType.STRING);
    public static final SearchableField ContentClaimOffset
            = new NamedSearchableField("ContentClaimOffset", "contentClaimOffset", "Content Claim Offset", false, SearchableFieldType.LONG);
    public static final SearchableField SourceQueueIdentifier
            = new NamedSearchableField("SourceQueueIdentifier", "sourceQueueIdentifier", "Source Queue Identifier", false, SearchableFieldType.STRING);

    private static final Map<String, SearchableField> standardFields;

    static {
        final SearchableField[] searchableFields = new SearchableField[]{
            EventTime, FlowFileUUID, Filename, EventType, TransitURI,
            ComponentID, AlternateIdentifierURI, FileSize, Relationship, Details,
            LineageStartDate, LineageIdentifier, ContentClaimSection, ContentClaimContainer, ContentClaimIdentifier,
            ContentClaimOffset, SourceQueueIdentifier};

        final Map<String, SearchableField> fields = new HashMap<>();
        for (final SearchableField field : searchableFields) {
            fields.put(field.getIdentifier(), field);
        }

        standardFields = Collections.unmodifiableMap(fields);
    }

    private SearchableFields() {
    }

    public static Collection<SearchableField> getStandardFields() {
        return standardFields.values();
    }

    public static SearchableField getSearchableField(final String fieldIdentifier) {
        return standardFields.get(fieldIdentifier);
    }

    public static SearchableField newSearchableAttribute(final String attributeName) {
        return new NamedSearchableField(attributeName, attributeName, attributeName, true);
    }
}
