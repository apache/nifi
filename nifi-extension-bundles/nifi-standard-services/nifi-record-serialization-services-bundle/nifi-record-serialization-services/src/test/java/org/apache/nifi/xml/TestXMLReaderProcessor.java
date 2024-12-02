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

package org.apache.nifi.xml;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.StringUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TestXMLReaderProcessor extends AbstractProcessor {

    static final PropertyDescriptor XML_READER = new PropertyDescriptor.Builder()
            .name("xml_reader")
            .description("xml_reader")
            .identifiesControllerService(XMLReader.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_FIELD_TO_GET_AS_STRING = new PropertyDescriptor.Builder()
            .name("record_field_to_get_as_string")
            .description("record_field_to_get_as_string")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success").description("success").build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        final RecordReaderFactory readerFactory = context.getProperty(XML_READER).asControllerService(RecordReaderFactory.class);
        final String recordFieldAsString = context.getProperty(RECORD_FIELD_TO_GET_AS_STRING).getValue();

        final List<String> records = new ArrayList<>();

        try (final InputStream in = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
            Record record;
            while ((record = reader.nextRecord()) != null) {
                if (recordFieldAsString == null) {
                    records.add(record.toString());
                } else {
                    records.add(record.getAsString(recordFieldAsString));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        flowFile = session.write(flowFile, (out) -> out.write(StringUtils.join(records, "\n").getBytes()));
        session.transfer(flowFile, SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(XML_READER, RECORD_FIELD_TO_GET_AS_STRING);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(SUCCESS);
    }
}
