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

import static org.apache.nifi.processors.airtable.parse.AirtableTableRetriever.JSON_FACTORY;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;

public class AirtableRecordSetFlowFileWriter {

    private final FlowFile flowFile;
    private final JsonGenerator jsonGenerator;
    private int recordCount = 0;

    private AirtableRecordSetFlowFileWriter(final FlowFile flowFile, final JsonGenerator jsonGenerator) {
        this.flowFile = flowFile;
        this.jsonGenerator = jsonGenerator;
    }

    public static AirtableRecordSetFlowFileWriter startRecordSet(final ProcessSession session) throws IOException {
        final FlowFile flowFile = session.create();
        final JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(session.write(flowFile));
        jsonGenerator.writeStartArray();
        return new AirtableRecordSetFlowFileWriter(flowFile, jsonGenerator);
    }

    public void writeRecord(final JsonParser jsonParser) throws IOException {
        recordCount++;
        jsonGenerator.copyCurrentStructure(jsonParser);
    }

    public FlowFile closeRecordSet(final ProcessSession session) throws IOException {
        jsonGenerator.writeEndArray();
        jsonGenerator.close();
        FlowFile flowFileWithAttributes = session.putAttribute(flowFile, "record.count", String.valueOf(recordCount));
        flowFileWithAttributes = session.putAttribute(flowFileWithAttributes, CoreAttributes.MIME_TYPE.key(), "application/json");
        return flowFileWithAttributes;
    }

    public int getRecordCount() {
        return recordCount;
    }
}
