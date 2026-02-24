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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.emptyList;

/**
  * Implementation of {@link ProvenanceRepository} that does not
  * store events.
  *
  */
public class NoOpProvenanceRepository implements ProvenanceRepository {

    @Override
    public void initialize(final EventReporter eventReporter, final Authorizer authorizer,
            final ProvenanceAuthorizableFactory factory, final IdentifierLookup identifierLookup)
            throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public ProvenanceEventBuilder eventBuilder() {
        return new StandardProvenanceEventRecord.Builder();
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id) throws IOException {
        return null;
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id, final NiFiUser user) throws IOException {
        return null;
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords)
            throws IOException {
        return emptyList();
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId,
            final int maxRecords, final NiFiUser niFiUser) throws IOException {
        return emptyList();
    }

    @Override
    public Long getMaxEventId() {
        return null;
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord records) {

    }

    @Override
    public void registerEvents(final Iterable<ProvenanceEventRecord> records) {

    }

    @Override
    public ProvenanceEventRepository getProvenanceEventRepository() {
        return this;
    }

    @Override
    public QuerySubmission submitQuery(final Query query, final NiFiUser niFiUser) {
        return null;
    }

    @Override
    public List<ProvenanceEventRecord> getLatestCachedEvents(final String componentId, final int eventLimit) {
        return List.of();
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier, final NiFiUser niFiUser) {
        return null;
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final String s, final NiFiUser niFiUser) {
        return null;
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final long eventId, final NiFiUser user) {
        return null;
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        return null;
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        return null;
    }

    @Override
    public Set<String> getContainerNames() {
        return EMPTY_SET;
    }

    @Override
    public long getContainerCapacity(final String s) throws IOException {
        return 0;
    }

    @Override
    public String getContainerFileStoreName(final String s) {
        return null;
    }

    @Override
    public long getContainerUsableSpace(final String s) throws IOException {
        return 0;
    }

    @Override
    public AsyncLineageSubmission retrieveLineageSubmission(final String lineageIdentifier,
            final NiFiUser user) {
        return null;
    }

    @Override
    public AsyncLineageSubmission submitExpandParents(final long eventId, final NiFiUser user) {
        return null;
    }

    @Override
    public AsyncLineageSubmission submitExpandChildren(final long eventId, final NiFiUser user) {
        return null;
    }

}
