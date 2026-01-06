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
package org.apache.nifi.processors.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"graph", "gremlin", "cypher", "enrich", "record"})
@CapabilityDescription("This processor uses fields from FlowFile records to add property values to nodes or edges in a graph. Each record is associated with an individual node/edge "
        + "(associated by the specified 'identifier' field value), and a single FlowFile will be output for all successful operations. Failed records will be sent as "
        + "individual FlowFiles to the failure relationship.")
@WritesAttributes({
        @WritesAttribute(attribute = EnrichGraphRecord.GRAPH_OPERATION_TIME, description = "The amount of time it took to execute all of the graph operations."),
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "Field(s) containing values to be added to the matched node/edge as properties. If no user-defined properties are added, all fields except the identifier "
        + "will be added as properties on the node/edge",
        value = "The variable name to be set", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "A dynamic property specifying a RecordField Expression identifying field(s) for whose values will be added to the matched node as properties")
public class EnrichGraphRecord extends AbstractGraphExecutor {

    private static final AllowableValue NODES = new AllowableValue(
            GraphClientService.NODES_TYPE,
            GraphClientService.NODES_TYPE,
            "Enrich nodes in the graph with properties from the incoming records. The node identifier is determined by the 'Identifier Field(s)' property."
    );

    private static final AllowableValue EDGES = new AllowableValue(
            GraphClientService.EDGES_TYPE,
            GraphClientService.EDGES_TYPE,
            "Enrich edges in the graph with properties from the incoming records. The edge identifier is determined by the 'Identifier Field(s)' property."
    );

    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Graph Client Service")
            .description("The graph client service for connecting to a graph database.")
            .identifiesControllerService(GraphClientService.class)
            .addValidator(Validator.VALID)
            .required(true)
            .build();

    public static final PropertyDescriptor READER_SERVICE = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The record reader to use with this processor to read incoming records.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor WRITER_SERVICE = new PropertyDescriptor.Builder()
            .name("Failed Record Writer")
            .description("The record writer to use for writing failed records.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor UPDATE_TYPE = new PropertyDescriptor.Builder()
            .name("Components to Enrich")
            .description("The components in the graph to enrich with properties from the incoming records.")
            .addValidator(Validator.VALID)
            .allowableValues(NODES, EDGES)
            .defaultValue(NODES.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor IDENTIFIER_FIELD = new PropertyDescriptor.Builder()
            .name("Identifier Field(s)")
            .description("A RecordPath Expression for field(s) in the record used to match the node identifier(s) in order to set properties on that node")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .build();

    public static final PropertyDescriptor NODE_TYPE = new PropertyDescriptor.Builder()
            .name("Node/Edge Type")
            .description("The type of the nodes or edges to match on. Setting this can result in faster execution")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    public static final Relationship ORIGINAL = new Relationship.Builder().name("original")
            .description("Original flow files that successfully interacted with " +
                    "graph server.")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder().name("failure")
            .description("Flow files that fail to interact with graph server.")
            .build();
    public static final Relationship GRAPH = new Relationship.Builder().name("response")
            .description("The response object from the graph server.")
            .autoTerminateDefault(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLIENT_SERVICE,
            READER_SERVICE,
            WRITER_SERVICE,
            UPDATE_TYPE,
            IDENTIFIER_FIELD,
            NODE_TYPE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            ORIGINAL,
            FAILURE,
            GRAPH
    );

    public static final String RECORD_COUNT = "record.count";
    public static final String GRAPH_OPERATION_TIME = "graph.operations.took";
    private volatile RecordPathCache recordPathCache;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private GraphClientService clientService;
    private RecordReaderFactory recordReaderFactory;
    private RecordSetWriterFactory recordSetWriterFactory;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(GraphClientService.class);
        recordReaderFactory = context.getProperty(READER_SERVICE).asControllerService(RecordReaderFactory.class);
        recordSetWriterFactory = context.getProperty(WRITER_SERVICE).asControllerService(RecordSetWriterFactory.class);
        recordPathCache = new RecordPathCache(100);
    }

    private List<FieldValue> getRecordValue(Record record, RecordPath recordPath) {
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> values = result.getSelectedFields().toList();
        return values.isEmpty() ? null : values;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        Map<String, RecordPath> dynamic = new HashMap<>();

        FlowFile finalInput = input;
        context.getProperties()
                .keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .forEach(it ->
                        dynamic.put(it.getName(), recordPathCache.getCompiled(
                                context
                                        .getProperty(it.getName())
                                        .evaluateAttributeExpressions(finalInput)
                                        .getValue()))
            );

        long delta;
        FlowFile failedRecords = session.create(input);
        WriteResult failedWriteResult = null;
        try (InputStream is = session.read(input);
             RecordReader reader = recordReaderFactory.createRecordReader(input, is, getLogger());
             OutputStream os = session.write(failedRecords);
             RecordSetWriter failedWriter = recordSetWriterFactory.createWriter(getLogger(), reader.getSchema(), os, input.getAttributes())
        ) {
            Record record;
            long start = System.currentTimeMillis();
            failedWriter.beginRecordSet();
            int records = 0;
            while ((record = reader.nextRecord()) != null) {
                FlowFile graph = session.create(input);

                try {
                    final String identifierField = context.getProperty(IDENTIFIER_FIELD).evaluateAttributeExpressions(input).getValue();
                    final RecordPath identifierPath = recordPathCache.getCompiled(identifierField);
                    final List<FieldValue> identifierValues = getRecordValue(record, identifierPath);
                    if (identifierValues == null || identifierValues.isEmpty()) {
                        throw new IOException("Identifier field(s) not found in record (check the RecordPath Expression), sending this record to failure");
                    }
                    Map<String, Object> dynamicPropertyMap = new HashMap<>();
                    Set<String> keySet = dynamic.keySet();
                    if (keySet.isEmpty()) {
                        // Add all dynamic properties at the top level except the identifier field
                        List<String> fieldNames = record.getSchema().getFieldNames();
                        for (String fieldName : fieldNames) {
                            if (fieldName.equals(identifierField)) {
                                continue;
                            }
                            final List<FieldValue> propertyValues = getRecordValue(record, recordPathCache.getCompiled("/" + fieldName));
                            // Use the first value if multiple are found
                            if (propertyValues == null || propertyValues.isEmpty() || propertyValues.getFirst().getValue() == null) {
                                continue;
                            }

                            Object rawValue = propertyValues.getFirst().getValue();
                            DataType rawDataType = propertyValues.getFirst().getField().getDataType();
                            RecordFieldType rawValueType = rawDataType.getFieldType();
                            // Change MapRecords to Maps recursively as needed
                            if (RecordFieldType.ARRAY.equals(rawValueType)) {
                                DataType arrayElementType = ((ArrayDataType) rawDataType).getElementType();
                                if (RecordFieldType.RECORD.getDataType().equals(arrayElementType)) {
                                    Object[] rawValueArray = (Object[]) rawValue;
                                    Object[] mappedValueArray = new Object[rawValueArray.length];
                                    for (int i = 0; i < rawValueArray.length; i++) {
                                        MapRecord mapRecord = (MapRecord) rawValueArray[i];
                                        mappedValueArray[i] = mapRecord.toMap(true);
                                    }
                                    dynamicPropertyMap.put(fieldName, mappedValueArray);
                                }
                            } else if (RecordFieldType.RECORD.equals(rawValueType)) {
                                MapRecord mapRecord = (MapRecord) rawValue;
                                dynamicPropertyMap.put(fieldName, mapRecord.toMap(true));
                            } else {
                                dynamicPropertyMap.put(fieldName, rawValue);
                            }
                        }
                    } else {
                        for (String entry : keySet) {
                            if (!dynamicPropertyMap.containsKey(entry)) {
                                final List<FieldValue> propertyValues = getRecordValue(record, dynamic.get(entry));
                                // Use the first value if multiple are found
                                if (propertyValues == null || propertyValues.isEmpty() || propertyValues.getFirst().getValue() == null) {
                                    throw new IOException("Dynamic property field(s) not found in record (check the RecordPath Expression), sending this record to failure");
                                }

                                dynamicPropertyMap.put(entry, propertyValues.getFirst().getValue());
                            }
                        }
                    }

                    final String nodeType = context.getProperty(NODE_TYPE).evaluateAttributeExpressions(input).getValue();
                    List<Tuple<String, String>> identifiersAndValues = new ArrayList<>(identifierValues.size());
                    for (FieldValue fieldValue : identifierValues) {
                        if (fieldValue.getValue() == null) {
                            throw new IOException(String.format("Identifier field '%s' is null for record at index %d, sending this record to failure", identifierField, records));
                        }
                        identifiersAndValues.add(new Tuple<>(fieldValue.getField().getFieldName(), fieldValue.getValue().toString()));
                    }

                    final String setStatement = clientService.generateSetPropertiesStatement(
                            GraphClientService.NODES_TYPE,
                            identifiersAndValues,
                            nodeType,
                            dynamicPropertyMap);

                    List<Map<String, Object>> graphResponses = new ArrayList<>(executeQuery(setStatement, dynamicPropertyMap));

                    OutputStream graphOutputStream = session.write(graph);
                    String graphOutput = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(graphResponses);
                    graphOutputStream.write(graphOutput.getBytes(StandardCharsets.UTF_8));
                    graphOutputStream.close();
                    session.transfer(graph, GRAPH);
                } catch (Exception e) {
                    getLogger().error("Error processing record at index {}", records, e);
                    // write failed records to a flowfile destined for the failure relationship
                    failedWriter.write(record);
                    session.remove(graph);
                } finally {
                    records++;
                }
            }
            long end = System.currentTimeMillis();
            delta = (end - start) / 1000;
            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Took %s seconds.\nHandled %d records", delta, records));
            }
            failedWriteResult = failedWriter.finishRecordSet();
            failedWriter.flush();

        } catch (Exception ex) {
            getLogger().error("Error reading records, routing input FlowFile to failure", ex);
            session.remove(failedRecords);
            session.transfer(input, FAILURE);
            return;
        }

        // Generate provenance and send input flowfile to success
        session.getProvenanceReporter().send(input, clientService.getTransitUrl(), delta * 1000);

        if (failedWriteResult.getRecordCount() < 1) {
            // No failed records, remove the failure flowfile and send the input flowfile to success
            session.remove(failedRecords);
            input = session.putAttribute(input, GRAPH_OPERATION_TIME, String.valueOf(delta));
            session.transfer(input, ORIGINAL);
        } else {
            failedRecords = session.putAttribute(failedRecords, RECORD_COUNT, String.valueOf(failedWriteResult.getRecordCount()));
            session.transfer(failedRecords, FAILURE);
            // There were failures, don't send the input FlowFile to SUCCESS
            session.remove(input);
        }
    }

    private List<Map<String, Object>> executeQuery(String recordScript, Map<String, Object> parameters) {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> graphResponses = new ArrayList<>();
        clientService.executeQuery(recordScript, parameters, (map, b) -> {
            if (getLogger().isDebugEnabled()) {
                try {
                    getLogger().debug(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map));
                } catch (JsonProcessingException ex) {
                    getLogger().error("Error converted map to JSON ", ex);
                }
            }
            graphResponses.add(map);
        });
        return graphResponses;
    }
}
