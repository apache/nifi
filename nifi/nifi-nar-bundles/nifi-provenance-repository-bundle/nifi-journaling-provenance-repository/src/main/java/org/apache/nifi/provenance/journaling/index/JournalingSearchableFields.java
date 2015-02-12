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
package org.apache.nifi.provenance.journaling.index;

import org.apache.nifi.provenance.NamedSearchableField;
import org.apache.nifi.provenance.search.SearchableField;

public class JournalingSearchableFields {
    public static SearchableField CONTAINER_NAME = new NamedSearchableField(IndexedFieldNames.CONTAINER_NAME, IndexedFieldNames.CONTAINER_NAME, "Container Name", false);
    public static SearchableField SECTION_NAME = new NamedSearchableField(IndexedFieldNames.SECTION_NAME, IndexedFieldNames.SECTION_NAME, "Section Name", false);
    public static SearchableField JOURNAL_ID = new NamedSearchableField(IndexedFieldNames.JOURNAL_ID, IndexedFieldNames.JOURNAL_ID, "Journal ID", false);
    public static SearchableField BLOCK_INDEX = new NamedSearchableField(IndexedFieldNames.BLOCK_INDEX, IndexedFieldNames.BLOCK_INDEX, "Block Index", false);
    public static SearchableField EVENT_ID = new NamedSearchableField(IndexedFieldNames.EVENT_ID, IndexedFieldNames.EVENT_ID, "Event ID", false);
    
}
