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
package org.apache.nifi.services.couchbase;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.services.couchbase.exception.CouchbaseDocNotFoundException;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Tags({"lookup", "enrich", "couchbase"})
@CapabilityDescription("Lookup a record from Couchbase Server associated with the specified key. The coordinates that are passed to the lookup must contain the key 'key'.")
public class CouchbaseRecordLookupService extends AbstractCouchbaseService implements RecordLookupService {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for parsing fetched document from Couchbase Server")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            COUCHBASE_CONNECTION_SERVICE,
            BUCKET_NAME,
            SCOPE_NAME,
            COLLECTION_NAME,
            RECORD_READER
    );

    private volatile RecordReaderFactory readerFactory;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        super.onEnabled(context);
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
    }

    @Override
    public Optional<Record> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
        final Object documentId = coordinates.get(KEY);

        if (documentId == null) {
            return Optional.empty();
        }

        CouchbaseGetResult result;
        try {
            result = couchbaseClient.getDocument(documentId.toString());
        } catch (final CouchbaseDocNotFoundException e) {
            return Optional.empty();
        } catch (final Exception e) {
            throw new LookupFailureException("Failed to look up record with Document ID [%s] in Couchbase.".formatted(documentId), e);
        }

        try (final InputStream input = new ByteArrayInputStream(result.resultContent())) {
            final long inputLength = result.resultContent().length;
            final Map<String, String> stringMap = coordinates.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> String.valueOf(e.getValue())
                    ));

            final RecordReader recordReader = readerFactory.createRecordReader(stringMap, input, inputLength, getLogger());
            return Optional.ofNullable(recordReader.nextRecord());
        } catch (final Exception e) {
            throw new LookupFailureException("Failed to parse the looked-up record with Document ID [%s]".formatted(documentId), e);
        }
    }
}
