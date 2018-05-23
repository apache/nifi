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
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestXMLRecordSetWriterProcessor extends AbstractProcessor {

    static final PropertyDescriptor XML_WRITER = new PropertyDescriptor.Builder()
            .name("xml_writer")
            .identifiesControllerService(XMLRecordSetWriter.class)
            .required(true)
            .build();

    static final PropertyDescriptor MULTIPLE_RECORDS = new PropertyDescriptor.Builder()
            .name("multiple_records")
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success").description("success").build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        final RecordSetWriterFactory writerFactory = context.getProperty(XML_WRITER).asControllerService(RecordSetWriterFactory.class);
        flowFile = session.write(flowFile, out -> {
            try {

                final RecordSchema schema = writerFactory.getSchema(null, null);

                boolean multipleRecords = Boolean.parseBoolean(context.getProperty(MULTIPLE_RECORDS).getValue());
                RecordSet recordSet = getRecordSet(multipleRecords);

                final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out);


                writer.write(recordSet);
                writer.flush();


            } catch (Exception e) {
                throw new ProcessException(e.getMessage());
            }

        });
        session.transfer(flowFile, SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<PropertyDescriptor>() {{ add(XML_WRITER); add(MULTIPLE_RECORDS); }};
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<Relationship>() {{ add(SUCCESS); }};
    }

    protected static RecordSet getRecordSet(boolean multipleRecords) {
        Object[] arrayVals = {1, null, 3};

        Map<String,Object> recordFields = new HashMap<>();
        recordFields.put("name1", "val1");
        recordFields.put("name2", null);
        recordFields.put("array_field", arrayVals);

        RecordSchema emptySchema = new SimpleRecordSchema(Collections.emptyList());

        List<Record> records = new ArrayList<>();
        records.add(new MapRecord(emptySchema, recordFields));

        if (multipleRecords) {
            records.add(new MapRecord(emptySchema, recordFields));
        }

        return new ListRecordSet(emptySchema, records);
    }



}
