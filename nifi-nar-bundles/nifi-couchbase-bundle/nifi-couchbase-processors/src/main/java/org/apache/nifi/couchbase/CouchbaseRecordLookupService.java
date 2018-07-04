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
package org.apache.nifi.couchbase;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.deps.io.netty.buffer.ByteBufInputStream;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.Tuple;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.DOCUMENT_TYPE;

@Tags({"lookup", "enrich", "couchbase"})
@CapabilityDescription("Lookup a record from Couchbase Server associated with the specified key."
        + " The coordinates that are passed to the lookup must contain the key 'key'.")
public class CouchbaseRecordLookupService extends AbstractCouchbaseLookupService implements RecordLookupService {

    private volatile RecordReaderFactory readerFactory;
    private volatile DocumentType documentType;

    private static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for parsing fetched document from Couchbase Server.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    @Override
    protected void addProperties(List<PropertyDescriptor> properties) {
        properties.add(DOCUMENT_TYPE);
        properties.add(RECORD_READER);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        documentType = DocumentType.valueOf(context.getProperty(DOCUMENT_TYPE).getValue());
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {

        final Bucket bucket = couchbaseClusterService.openBucket(bucketName);
        final Optional<String> docId = Optional.ofNullable(coordinates.get(KEY)).map(Object::toString);

        final Optional<InputStream> inputStream;
        try {
            switch (documentType) {

                case Binary:
                    inputStream = docId
                            .map(key -> bucket.get(key, BinaryDocument.class))
                            .map(doc -> new ByteBufInputStream(doc.content()));
                    break;

                case Json:
                    inputStream= docId
                            .map(key -> bucket.get(key, RawJsonDocument.class))
                            .map(doc -> new ByteArrayInputStream(doc.content().getBytes(StandardCharsets.UTF_8)));
                    break;

                default:
                    return Optional.empty();
            }
        } catch (CouchbaseException e) {
            throw new LookupFailureException("Failed to lookup from Couchbase using this coordinates: " + coordinates);
        }

        final Optional<Tuple<Exception, RecordReader>> errOrReader = inputStream.map(in -> {
            try {
                // Pass coordinates to initiate RecordReader, so that the reader can resolve schema dynamically.
                // This allow using the same RecordReader service with different schemas if RecordReader is configured to
                // access schema based on Expression Language.
                final Map<String, String> recordReaderVariables = new HashMap<>(coordinates.size());
                coordinates.keySet().forEach(k -> {
                    final Object value = coordinates.get(k);
                    if (value != null) {
                        recordReaderVariables.put(k, value.toString());
                    }
                });
                return new Tuple<>(null, readerFactory.createRecordReader(recordReaderVariables, in, getLogger()));
            } catch (Exception e) {
                return new Tuple<>(e, null);
            }
        });

        if (!errOrReader.isPresent()) {
            return Optional.empty();
        }

        final Exception exception = errOrReader.get().getKey();
        if (exception != null) {
            throw new LookupFailureException(String.format("Failed to lookup with %s", coordinates), exception);
        }

        try {
            return Optional.ofNullable(errOrReader.get().getValue().nextRecord());
        } catch (Exception e) {
            throw new LookupFailureException(String.format("Failed to read Record when looking up with %s", coordinates), e);
        }
    }

}
