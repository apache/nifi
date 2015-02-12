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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StorageLocation;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.search.SearchTerm;

public class QueryUtils {
    public static org.apache.lucene.search.Query convertQueryToLucene(final org.apache.nifi.provenance.search.Query query) {
        if (query.getStartDate() == null && query.getEndDate() == null && query.getSearchTerms().isEmpty()) {
            return new MatchAllDocsQuery();
        }

        final BooleanQuery luceneQuery = new BooleanQuery();
        for (final SearchTerm searchTerm : query.getSearchTerms()) {
            final String searchValue = searchTerm.getValue();
            if (searchValue == null) {
                throw new IllegalArgumentException("Empty search value not allowed (for term '" + searchTerm.getSearchableField().getFriendlyName() + "')");
            }

            if (searchValue.contains("*") || searchValue.contains("?")) {
                luceneQuery.add(new BooleanClause(new WildcardQuery(new Term(searchTerm.getSearchableField().getSearchableFieldName(), searchTerm.getValue().toLowerCase())), Occur.MUST));
            } else {
                luceneQuery.add(new BooleanClause(new TermQuery(new Term(searchTerm.getSearchableField().getSearchableFieldName(), searchTerm.getValue().toLowerCase())), Occur.MUST));
            }
        }

        final Long minBytes = query.getMinFileSize() == null ? null : DataUnit.parseDataSize(query.getMinFileSize(), DataUnit.B).longValue();
        final Long maxBytes = query.getMaxFileSize() == null ? null : DataUnit.parseDataSize(query.getMaxFileSize(), DataUnit.B).longValue();
        if (minBytes != null || maxBytes != null) {
            luceneQuery.add(NumericRangeQuery.newLongRange(SearchableFields.FileSize.getSearchableFieldName(), minBytes, maxBytes, true, true), Occur.MUST);
        }

        final Long minDateTime = query.getStartDate() == null ? null : query.getStartDate().getTime();
        final Long maxDateTime = query.getEndDate() == null ? null : query.getEndDate().getTime();
        if (maxDateTime != null || minDateTime != null) {
            luceneQuery.add(NumericRangeQuery.newLongRange(SearchableFields.EventTime.getSearchableFieldName(), minDateTime, maxDateTime, true, true), Occur.MUST);
        }

        return luceneQuery;
    }
    
    
    private static File getJournalFile(final JournaledStorageLocation location, final JournalingRepositoryConfig config) throws FileNotFoundException {
        final File containerDir = config.getContainers().get(location.getContainerName());
        if ( containerDir == null ) {
            throw new FileNotFoundException("Could not find Container with name " + location.getContainerName());
        }
        
        final String sectionName = location.getSectionName();
        final File sectionFile = new File(containerDir, sectionName);
        final File journalDir = new File(sectionFile, "journals");
        final File journalFile = new File(journalDir, location.getJournalId() + ".journal");
        
        return journalFile;
    }
    
    
    public static Map<File, List<JournaledStorageLocation>> orderLocations(final List<StorageLocation> locations, final JournalingRepositoryConfig config) throws FileNotFoundException, IOException {
        final Map<File, List<JournaledStorageLocation>> map = new HashMap<>();
        
        for ( final StorageLocation location : locations ) {
            if ( !(location instanceof JournaledStorageLocation) ) {
                throw new IllegalArgumentException(location + " is not a valid StorageLocation for this repository");
            }
            
            final JournaledStorageLocation journaledLocation = (JournaledStorageLocation) location;
            final File journalFile = getJournalFile(journaledLocation, config);
            List<JournaledStorageLocation> list = map.get(journalFile);
            if ( list == null ) {
                list = new ArrayList<>();
                map.put(journalFile, list);
            }
            
            list.add(journaledLocation);
        }
        
        for ( final List<JournaledStorageLocation> list : map.values() ) {
            Collections.sort(list);
        }
        
        return map;
    }
    
    public static File getTocFile(final File journalFile) {
        return new File(journalFile.getParentFile(), journalFile.getName() + ".toc");
    }

}
