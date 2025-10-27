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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.json.JsonParserFactory;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.json.SchemaApplicationStrategy;
import org.apache.nifi.json.StartingFieldStrategy;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.schema.access.InferenceSchemaStrategy;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Tags({"lookup", "enrich", "couchbase"})
@CapabilityDescription("Lookup a record from Couchbase Server associated with the specified key. The coordinates that are passed to the lookup must contain the key 'key'.")
public class CouchbaseRecordLookupService extends AbstractCouchbaseService implements RecordLookupService {

    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss.SSSZ";
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static final JsonParserFactory jsonParserFactory = new JsonParserFactory();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            COUCHBASE_CONNECTION_SERVICE,
            BUCKET_NAME,
            SCOPE_NAME,
            COLLECTION_NAME
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        final Object documentId = coordinates.get(KEY);

        if (documentId == null) {
            return Optional.empty();
        }

        try {
            final CouchbaseGetResult result = couchbaseClient.getDocument(documentId.toString());
            final RecordSchema schema = new InferenceSchemaStrategy().getSchema(null, new ByteArrayInputStream(result.resultContent()), null);

            final JsonTreeRowRecordReader recordReader = createJsonReader(new ByteArrayInputStream(result.resultContent()), schema);

            return Optional.ofNullable(recordReader.nextRecord());
        } catch (Exception e) {
            throw new LookupFailureException("Record lookup from Couchbase failed", e);
        }
    }

    private JsonTreeRowRecordReader createJsonReader(InputStream inputStream, RecordSchema recordSchema) throws IOException, MalformedRecordException {
        return new JsonTreeRowRecordReader(
                inputStream,
                getLogger(),
                recordSchema,
                DATE_FORMAT,
                TIME_FORMAT,
                DATE_TIME_FORMAT,
                StartingFieldStrategy.ROOT_NODE,
                null,
                SchemaApplicationStrategy.SELECTED_PART,
                null,
                jsonParserFactory
        );
    }
}
