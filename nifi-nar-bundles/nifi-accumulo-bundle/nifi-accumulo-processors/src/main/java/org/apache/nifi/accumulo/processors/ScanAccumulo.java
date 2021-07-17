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
package org.apache.nifi.accumulo.processors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.nifi.accumulo.controllerservices.BaseAccumuloService;
import org.apache.nifi.accumulo.data.KeySchema;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Scan the given table and writes result in a flowfile. Value will be represented as UTF-8 Encoded String.")
@Tags({"hadoop", "accumulo", "scan", "record"})
/**
 * Purpose and Design: Requires a connector be defined by way of an AccumuloService object. This class
 * simply extends BaseAccumuloProcessor to scan accumulo based on aspects and expression executed against
 * a flow file
 *
 */
public class ScanAccumulo extends BaseAccumuloProcessor {

    static final PropertyDescriptor START_KEY = new PropertyDescriptor.Builder()
            .displayName("Start key")
            .name("start-key")
            .description("Start row key")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor START_KEY_INCLUSIVE = new PropertyDescriptor.Builder()
            .displayName("Start key Inclusive")
            .name("start-key-inclusive")
            .description("Determines if the start key is inclusive ")
            .required(false)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor END_KEY = new PropertyDescriptor.Builder()
            .displayName("End key")
            .name("end-key")
            .description("End row key for this. If not specified or empty this will be infinite")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor END_KEY_INCLUSIVE = new PropertyDescriptor.Builder()
            .displayName("End key Inclusive")
            .name("end-key-inclusive")
            .description("Determines if the end key is inclusive")
            .required(false)
            .defaultValue("False")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor AUTHORIZATIONS = new PropertyDescriptor.Builder()
            .name("accumulo-authorizations")
            .displayName("Authorizations")
            .description("The comma separated list of authorizations to pass to the scanner.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor COLUMNFAMILY = new PropertyDescriptor.Builder()
            .name("column-family")
            .displayName("Start Column Family")
            .description("The column family that is part of the start key. If no column key is defined only this column family will be selected")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor COLUMNFAMILY_END = new PropertyDescriptor.Builder()
            .name("column-family-end")
            .displayName("End Column Family")
            .description("The column family to select is part of end key")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor VALUE_INCLUDED_IN_RESULT = new PropertyDescriptor.Builder()
            .displayName("Value Included in Result")
            .name("accumulo-value-inclusive")
            .description("Beside keys and their values, accumulo value field will also be included in the result as UTF-8 Encoded String.")
            .required(false)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully retrieved from Accumulo")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be retrieved fromAccumulo")
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    /**
     * Connector service which provides us a connector if the configuration is correct.
     */
    protected BaseAccumuloService accumuloConnectorService;

    /**
     * Connector that we need to persist while we are operational.
     */
    protected AccumuloClient client;


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = new ArrayList<>();
        if ((validationContext.getProperty(COLUMNFAMILY).isSet() && !validationContext.getProperty(COLUMNFAMILY_END).isSet())
        || !validationContext.getProperty(COLUMNFAMILY).isSet() && validationContext.getProperty(COLUMNFAMILY_END).isSet() )
            set.add(new ValidationResult.Builder().explanation("Column Family and Column family end  must be defined").build());
        return set;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        accumuloConnectorService = context.getProperty(ACCUMULO_CONNECTOR_SERVICE).asControllerService(BaseAccumuloService.class);
        this.client = accumuloConnectorService.getClient();
    }

    private Authorizations stringToAuth(final String authorizations){
        if (!StringUtils.isBlank(authorizations))
            return  new Authorizations(authorizations.split(","));
        else
            return new Authorizations();
    }


    protected long scanAccumulo(final RecordSetWriterFactory writerFactory, final ProcessContext processContext, final ProcessSession processSession, final Optional<FlowFile> incomingFlowFile){

        final Map<String, String> flowAttributes = incomingFlowFile.isPresent() ?  incomingFlowFile.get().getAttributes() : new HashMap<>();
        final String table = processContext.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowAttributes).getValue();
        final String startKey = processContext.getProperty(START_KEY).evaluateAttributeExpressions(flowAttributes).getValue();
        final boolean startKeyInclusive = processContext.getProperty(START_KEY_INCLUSIVE).asBoolean();
        final boolean endKeyInclusive = processContext.getProperty(END_KEY_INCLUSIVE).asBoolean();
        final String endKey = processContext.getProperty(END_KEY).evaluateAttributeExpressions(flowAttributes).getValue();
        final String authorizations = processContext.getProperty(AUTHORIZATIONS).isSet()
                ? processContext.getProperty(AUTHORIZATIONS).evaluateAttributeExpressions(flowAttributes).getValue() : "";
        final int threads = processContext.getProperty(THREADS).asInteger();
        final String startKeyCf = processContext.getProperty(COLUMNFAMILY).evaluateAttributeExpressions(flowAttributes).getValue();
        final String endKeyCf = processContext.getProperty(COLUMNFAMILY_END).evaluateAttributeExpressions(flowAttributes).getValue();
        final boolean valueIncluded = processContext.getProperty(VALUE_INCLUDED_IN_RESULT).asBoolean();

        final Authorizations auths = stringToAuth(authorizations);

        final LongAdder recordCounter = new LongAdder();

        final Range lookupRange = buildRange(startKey,startKeyCf,startKeyInclusive,endKey,endKeyCf,endKeyInclusive);

        boolean cloneFlowFile = incomingFlowFile.isPresent();

        accumuloConnectorService.renewTgtIfNecessary();

        try (BatchScanner scanner = client.createBatchScanner(table,auths,threads)) {
            if (!StringUtils.isBlank(startKeyCf) &&  StringUtils.isBlank(endKeyCf))
                scanner.fetchColumnFamily(new Text(startKeyCf));
            scanner.setRanges(Collections.singleton(lookupRange));
            scanner.setTimeout(processContext.getProperty(ACCUMULO_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).longValue(), TimeUnit.SECONDS);

            final Iterator<Map.Entry<Key,Value>> kvIter = scanner.iterator();
            if (!kvIter.hasNext()){
                /**
                 * Create a flow file with a record count of zero.
                 */
                final Map<String, String> attributes = new HashMap<>();
                attributes.put("record.count", String.valueOf(0));
                final FlowFile newFlow = processSession.create();
                processSession.putAllAttributes(newFlow,attributes);
                processSession.transfer(newFlow, REL_SUCCESS);
                return 0;
            } else{

                while (kvIter.hasNext()) {
                    FlowFile iterationFlowFile = cloneFlowFile ? processSession.clone(incomingFlowFile.get()) : processSession.create();

                    final int keysPerFlowFile = 1000;
                    final Map<String, String> attributes = new HashMap<>();
                    iterationFlowFile = processSession.write(iterationFlowFile, new StreamCallback() {
                        @Override
                        public void process(final InputStream in, final OutputStream out) throws IOException {

                            try{
                                final RecordSchema writeSchema = determineRecordSchema(writerFactory, flowAttributes, valueIncluded);

                                try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, Collections.emptyMap())) {

                                    int i = 0;
                                    writer.beginRecordSet();
                                    for (; i < keysPerFlowFile && kvIter.hasNext(); i++) {

                                        Map.Entry<Key, Value> kv = kvIter.next();

                                        final Key key = kv.getKey();

                                        Map<String, Object> data = new HashMap<>();
                                        data.put("row", key.getRow().toString());
                                        data.put("columnFamily", key.getColumnFamily().toString());
                                        data.put("columnQualifier", key.getColumnQualifier().toString());
                                        data.put("columnVisibility", key.getColumnVisibility().toString());
                                        data.put("timestamp", key.getTimestamp());
                                        if (valueIncluded) {
                                            data.put("value", Objects.isNull(kv.getValue()) ? null : kv.getValue().toString());
                                        }

                                        Record record = new MapRecord(writeSchema, data);
                                        writer.write(record);


                                    }
                                    recordCounter.add(i);

                                    final WriteResult writeResult = writer.finishRecordSet();
                                    attributes.put("record.count", String.valueOf(i));
                                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                    attributes.putAll(writeResult.getAttributes());
                                }
                            } catch (SchemaNotFoundException e) {
                                getLogger().error("Failed to process {}; will route to failure", new Object[] {
                                        incomingFlowFile.isPresent() ? incomingFlowFile.get() : "No incoming flow file", e});

                                throw new IOException(e);
                            }
                        }

                    });
                    processSession.putAllAttributes(iterationFlowFile,attributes);
                    processSession.transfer(iterationFlowFile, REL_SUCCESS);
                }
            }
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", new Object[] {incomingFlowFile.isPresent() ? incomingFlowFile.get() : "No incoming flow file", e});
            if (cloneFlowFile) {
                processSession.transfer(incomingFlowFile.get(), REL_FAILURE);
            }
            return 0;
        }

        if (cloneFlowFile) {
            processSession.remove(incomingFlowFile.get());
        }

        getLogger().info("Successfully converted {} records for {}", new Object[] {recordCounter.longValue(), incomingFlowFile.toString()});

        return recordCounter.longValue();
    }

    private RecordSchema determineRecordSchema(RecordSetWriterFactory writerFactory, Map<String, String> flowAttributes, boolean valueIncluded) throws SchemaNotFoundException, IOException {
        final RecordSchema writeSchema = writerFactory.getSchema(flowAttributes, new KeySchema());

        if (valueIncluded) {
            final List<RecordField> recordSchemaFields = new ArrayList<>();
            recordSchemaFields.addAll(writeSchema.getFields());
            recordSchemaFields.add(new RecordField("value", RecordFieldType.STRING.getDataType()));
            return new SimpleRecordSchema(recordSchemaFields);
        }
        return writeSchema;
    }


    Range buildRange(final String startRow, final String startKeyCf,boolean startKeyInclusive, final String endRow, final String endKeyCf,boolean endKeyInclusive){
        Key start = StringUtils.isBlank(startRow) ? null : StringUtils.isBlank(startKeyCf) ? new Key(startRow) : new Key(startRow,startKeyCf);
        Key end = StringUtils.isBlank(endRow) ? null : StringUtils.isBlank(endKeyCf) ? new Key(endRow) : new Key(endRow,endKeyCf);
        return new Range(start,startKeyInclusive,end,endKeyInclusive);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();

        final RecordSetWriterFactory writerFactory = processContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        long recordCount = scanAccumulo(writerFactory,processContext,processSession,Optional.ofNullable(flowFile));

        processSession.adjustCounter("Records Processed", recordCount, false);
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(baseProperties);
        properties.add(START_KEY);
        properties.add(START_KEY_INCLUSIVE);
        properties.add(END_KEY);
        properties.add(COLUMNFAMILY);
        properties.add(COLUMNFAMILY_END);
        properties.add(END_KEY_INCLUSIVE);
        properties.add(VALUE_INCLUDED_IN_RESULT);
        properties.add(RECORD_WRITER);
        properties.add(AUTHORIZATIONS);
        return properties;
    }

}
