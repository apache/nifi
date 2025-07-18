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

package org.apache.nifi.processors.airtable.parse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.airtable.service.AirtableGetRecordsParameters;
import org.apache.nifi.processors.airtable.service.AirtableRestService;

public class AirtableTableRetriever {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory()
            .configure(Feature.AUTO_CLOSE_JSON_CONTENT, false);

    final AirtableRestService airtableRestService;
    final AirtableGetRecordsParameters getRecordsParameters;
    final Integer maxRecordsPerFlowFile;

    public AirtableTableRetriever(final AirtableRestService airtableRestService,
            final AirtableGetRecordsParameters getRecordsParameters,
            final Integer maxRecordsPerFlowFile) {
        this.airtableRestService = airtableRestService;
        this.getRecordsParameters = getRecordsParameters;
        this.maxRecordsPerFlowFile = maxRecordsPerFlowFile;
    }

    public AirtableRetrieveTableResult retrieveAll(final ProcessSession session) throws IOException {
        int totalRecordCount = 0;
        final List<FlowFile> flowFiles = new ArrayList<>();
        AirtableRetrievePageResult retrievePageResult = null;
        do {
            retrievePageResult = retrieveNextPage(session, Optional.ofNullable(retrievePageResult));
            totalRecordCount += retrievePageResult.getParsedRecordCount();
            flowFiles.addAll(retrievePageResult.getFlowFiles());
        } while (retrievePageResult.getNextOffset().isPresent());

        retrievePageResult.getOngoingRecordSetFlowFileWriter()
                .map(writer -> {
                    try {
                        return writer.closeRecordSet(session);
                    } catch (IOException e) {
                        throw new ProcessException("Failed to close Airtable record writer", e);
                    }
                })
                .ifPresent(flowFiles::add);
        return new AirtableRetrieveTableResult(flowFiles, totalRecordCount);
    }

    private AirtableRetrievePageResult retrieveNextPage(final ProcessSession session, final Optional<AirtableRetrievePageResult> previousPageResult) {
        final AirtableGetRecordsParameters parameters = previousPageResult.flatMap(AirtableRetrievePageResult::getNextOffset)
                .map(getRecordsParameters::withOffset)
                .orElse(getRecordsParameters);

        return airtableRestService.getRecords(parameters, inputStream -> parsePage(inputStream, session, previousPageResult));
    }

    private AirtableRetrievePageResult parsePage(final InputStream inputStream, final ProcessSession session, final Optional<AirtableRetrievePageResult> previousPageResult) {
        final List<FlowFile> flowFiles = new ArrayList<>();
        AirtableRecordSetFlowFileWriter flowFileWriter = previousPageResult.flatMap(AirtableRetrievePageResult::getOngoingRecordSetFlowFileWriter)
                        .orElse(null);
        int parsedRecordCount = 0;
        String nextOffset = null;
        try (final JsonParser jsonParser = JSON_FACTORY.createParser(inputStream)) {
            while (jsonParser.nextToken() != null) {
                if (jsonParser.currentToken() != JsonToken.FIELD_NAME) {
                    continue;
                }
                switch (jsonParser.currentName()) {
                    case "records":
                        jsonParser.nextToken();
                        if (jsonParser.currentToken() != JsonToken.START_ARRAY) {
                            break;
                        }
                        while (jsonParser.nextToken() != null && jsonParser.currentToken() != JsonToken.END_ARRAY) {
                            if (jsonParser.currentToken() != JsonToken.START_OBJECT) {
                                continue;
                            }
                            if (flowFileWriter == null) {
                                flowFileWriter = AirtableRecordSetFlowFileWriter.startRecordSet(session);
                            }
                            ++parsedRecordCount;
                            flowFileWriter.writeRecord(jsonParser);
                            if (maxRecordsPerFlowFile != null && maxRecordsPerFlowFile == flowFileWriter.getRecordCount()) {
                                flowFiles.add(flowFileWriter.closeRecordSet(session));
                                flowFileWriter = null;
                            }

                        }
                        break;
                    case "offset":
                        jsonParser.nextToken();
                        nextOffset = jsonParser.getValueAsString();
                        break;
                }
            }
        } catch (final IOException e) {
            throw new ProcessException("Failed to parse Airtable query table response page", e);
        }
        return new AirtableRetrievePageResult(Optional.ofNullable(nextOffset), flowFiles, parsedRecordCount, Optional.ofNullable(flowFileWriter));
    }
}
