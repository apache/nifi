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

package org.apache.nifi.text;

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
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestFreeFormTextRecordSetWriterProcessor extends AbstractProcessor {

    static final PropertyDescriptor WRITER = new PropertyDescriptor.Builder()
            .name("writer")
            .identifiesControllerService(FreeFormTextRecordSetWriter.class)
            .required(true)
            .build();

    static final PropertyDescriptor MULTIPLE_RECORDS = new PropertyDescriptor.Builder()
            .name("multiple_records")
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success").description("success").build();

    private static final RecordSchema recordSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("ID", RecordFieldType.STRING.getDataType()),
            new RecordField("NAME", RecordFieldType.STRING.getDataType()),
            new RecordField("AGE", RecordFieldType.INT.getDataType()),
            new RecordField("COUNTRY", RecordFieldType.STRING.getDataType())));

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        final RecordSetWriterFactory writerFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
        final FlowFile flowFileRef = flowFile;
        flowFile = session.write(flowFile, out -> {
            try {
                // The "reader" RecordSchema must be passed in here as the controller service expects to inherit it from the record itself
                // See the InheritSchemaFromRecord class for more details
                final RecordSchema schema = writerFactory.getSchema(flowFileRef.getAttributes(), recordSchema);

                boolean multipleRecords = Boolean.parseBoolean(context.getProperty(MULTIPLE_RECORDS).getValue());
                RecordSet recordSet = getRecordSet(multipleRecords);

                final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, flowFileRef);

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
        return new ArrayList<PropertyDescriptor>() {{
            add(WRITER);
            add(MULTIPLE_RECORDS);
        }};
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<Relationship>() {{
            add(SUCCESS);
        }};
    }

    protected static RecordSet getRecordSet(boolean multipleRecords) {

        Map<String, Object> recordFields = new HashMap<>();
        recordFields.put("ID", "ABC123");
        recordFields.put("NAME", "John Doe");
        recordFields.put("AGE", 22);
        recordFields.put("COUNTRY", "USA");
        // Username is an additional "field" in the output but is not present in the record and will be supplied by an attribute for the test(s).

        List<Record> records = new ArrayList<>();
        records.add(new MapRecord(recordSchema, recordFields));

        if (multipleRecords) {
            records.add(new MapRecord(recordSchema, recordFields));
        }

        return new ListRecordSet(recordSchema, records);
    }
}
