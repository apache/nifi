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

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.record.path.RecordPath
import org.apache.nifi.record.path.RecordPathResult
import org.apache.nifi.serialization.*
import org.apache.nifi.serialization.record.*
import org.apache.nifi.schema.access.SchemaNotFoundException
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

class MyRecordProcessor extends AbstractProcessor {

    // Properties
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build()

    static final PropertyDescriptor RECORD_PATH = new PropertyDescriptor.Builder()
            .name("record-path")
            .displayName("Record Path")
            .description("Specifies the Record Path expression to evaluate against each record")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build()

    def REL_SUCCESS = new Relationship.Builder().name("success").description('FlowFiles that were successfully processed are routed here').build()
    def REL_FAILURE = new Relationship.Builder().name("failure").description('FlowFiles are routed here if an error occurs during processing').build()

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        def properties = [] as ArrayList
        properties.add(RECORD_READER)
        properties.add(RECORD_PATH)
        properties
    }

    @Override
    Set<Relationship> getRelationships() {
        [REL_SUCCESS, REL_FAILURE] as Set<Relationship>
    }

    @Override
    void onTrigger(ProcessContext context, ProcessSession session) {
        def flowFile = session.get()
        if (!flowFile) return

        def readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory)

        final Map<String, String> attributes = new HashMap<>()
        final FlowFile original = flowFile
        final Map<String, String> originalAttributes = flowFile.attributes
        final String recordPathExpression = context.getProperty(RECORD_PATH).getValue()
        final RecordPath recordPath = RecordPath.compile(recordPathExpression)
        try {
            flowFile = session.write(flowFile, { inStream, outStream ->
                def reader = readerFactory.createRecordReader(originalAttributes, inStream, 100, getLogger())
                try {
                    Record record
                    while (record = reader.nextRecord()) {
                        RecordPathResult result = recordPath.evaluate(record)
                        def line = result.selectedFields.map({f -> record.getAsString(f.field.fieldName).toString()}).collect(Collectors.joining(',')) + '\n'
                        outStream.write(line.bytes)
                    }

                } catch (final SchemaNotFoundException e) {
                    throw new ProcessException(e.localizedMessage, e)
                } catch (final MalformedRecordException e) {
                    throw new ProcessException('Could not parse incoming data', e)
                } finally {
                    reader.close()
                }
            } as StreamCallback)

        } catch (final Exception e) {
            getLogger().error('Failed to process {}; will route to failure', [flowFile, e] as Object[])
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        flowFile = session.putAllAttributes(flowFile, attributes)
        session.transfer(flowFile, REL_SUCCESS)
    }
}

processor = new MyRecordProcessor()