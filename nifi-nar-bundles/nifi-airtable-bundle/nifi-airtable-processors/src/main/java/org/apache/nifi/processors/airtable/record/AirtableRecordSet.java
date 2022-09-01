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

package org.apache.nifi.processors.airtable.record;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.processors.airtable.service.AirtableGetRecordsParameters;
import org.apache.nifi.processors.airtable.service.AirtableRestService;
import org.apache.nifi.processors.airtable.service.AirtableRestService.RateLimitExceededException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

public class AirtableRecordSet implements RecordSet, Closeable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();

    final AirtableJsonTreeRowRecordReaderFactory recordReaderFactory;
    final AirtableRestService restService;
    final AirtableGetRecordsParameters getRecordsParameters;
    byte[] recordsJson;
    JsonTreeRowRecordReader reader = null;

    public AirtableRecordSet(final byte[] recordsJson,
            final AirtableJsonTreeRowRecordReaderFactory recordReaderFactory,
            final AirtableRestService restService,
            final AirtableGetRecordsParameters getRecordsParameters) {
        this.recordReaderFactory = recordReaderFactory;
        this.restService = restService;
        this.getRecordsParameters = getRecordsParameters;
        this.recordsJson = recordsJson;
    }

    @Override
    public RecordSchema getSchema() {
        return recordReaderFactory.recordSchema;
    }

    @Override
    public Record next() throws IOException {
        if (reader == null) {
            final ByteArrayInputStream inputStream = new ByteArrayInputStream(recordsJson);
            try {
                reader = recordReaderFactory.create(inputStream);
            } catch (MalformedRecordException e) {
                throw new IOException("Failed to create Airtable record reader", e);
            }
        }
        final Record record;
        try {
            record = reader.nextRecord();
        } catch (MalformedRecordException e) {
            throw new IOException("Failed to read next Airtable record", e);
        }

        if (record != null) {
            return record;
        }

        final Optional<String> offset = readOffsetFromJson();
        if (offset.isPresent()) {
            try {
                recordsJson = restService.getRecords(getRecordsParameters.withOffset(offset.get()));
            } catch (RateLimitExceededException e) {
                throw new IOException("REST API rate limit exceeded while fetching Airtable records", e);
            }
            reader = null;
            return next();
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    private Optional<String> readOffsetFromJson() throws IOException {
        try (final JsonParser jsonParser = JSON_FACTORY.createParser(recordsJson)) {
            while (jsonParser.nextToken() != null) {
                if (jsonParser.getCurrentToken() != JsonToken.FIELD_NAME || !jsonParser.getCurrentName().equals("offset")) {
                    continue;
                }
                jsonParser.nextToken();
                return Optional.of(jsonParser.getValueAsString());
            }
        }
        return Optional.empty();
    }
}