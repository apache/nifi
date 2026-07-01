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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.graph.GraphElementType;
import org.apache.nifi.graph.GraphClientTransientException;
import org.apache.nifi.graph.GraphMutation;
import org.apache.nifi.graph.GraphQueryGeneratorService;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Tags({"graph", "gremlin", "cypher", "enrich", "record"})
@CapabilityDescription("This processor uses fields from FlowFile records to add property values to graph elements. Each record is associated "
        + "with an individual graph element using the specified identifier field values. A single FlowFile containing successful graph responses is "
        + "written to the response relationship. Failed records are written to a single FlowFile routed to the failure relationship.")
@WritesAttributes({
        @WritesAttribute(attribute = EnrichGraphRecord.GRAPH_OPERATIONS_TIME_SECONDS, description = "The amount of time in seconds that it took to execute all graph operations."),
        @WritesAttribute(attribute = EnrichGraphRecord.RECORD_COUNT, description = "The number of records unsuccessfully processed.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "Field(s) containing values to be added to matched elements as properties. If no user-defined properties are added, all fields "
        + "except identifier fields are added as element properties.",
        value = "The property name to be set in the graph query",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "A dynamic property specifying a RecordPath Expression identifying field(s) whose values are added as properties")
public class EnrichGraphRecord extends AbstractGraphExecutor {
    private static final AllowableValue NODE = new AllowableValue(
            GraphElementType.NODE.name(),
            "Node",
            "Enrich nodes in the graph with properties from incoming records."
    );

    private static final AllowableValue EDGE = new AllowableValue(
            GraphElementType.EDGE.name(),
            "Edge",
            "Enrich edges in the graph with properties from incoming records."
    );

    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Graph Client Service")
            .description("The graph client service for connecting to a graph database.")
            .identifiesControllerService(GraphClientService.class)
            .addValidator(Validator.VALID)
            .required(true)
            .build();

    public static final PropertyDescriptor QUERY_GENERATOR_SERVICE = new PropertyDescriptor.Builder()
            .name("Graph Query Generator Service")
            .description("The graph query generator service used to build mutation statements for the selected graph implementation.")
            .identifiesControllerService(GraphQueryGeneratorService.class)
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

    public static final PropertyDescriptor ELEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("Element Type")
            .description("The graph element type to enrich with properties from incoming records.")
            .addValidator(Validator.VALID)
            .allowableValues(NODE, EDGE)
            .defaultValue(NODE.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor IDENTIFIER_FIELD = new PropertyDescriptor.Builder()
            .name("Identifier Field(s)")
            .description("A RecordPath Expression for field(s) in the record used to match identifiers when setting properties.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELEMENT_LABEL = new PropertyDescriptor.Builder()
            .name("Element Label")
            .description("The graph element label used for matching in the graph query. Setting this can result in faster execution.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original FlowFiles that successfully interacted with graph server.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that fail to interact with graph server.")
            .build();

    public static final Relationship RESPONSE = new Relationship.Builder()
            .name("response")
            .description("The response object from the graph server.")
            .autoTerminateDefault(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLIENT_SERVICE,
            QUERY_GENERATOR_SERVICE,
            READER_SERVICE,
            WRITER_SERVICE,
            ELEMENT_TYPE,
            IDENTIFIER_FIELD,
            ELEMENT_LABEL
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            ORIGINAL,
            FAILURE,
            RESPONSE
    );

    public static final String RECORD_COUNT = "record.count";
    public static final String GRAPH_OPERATIONS_TIME_SECONDS = "graph.operations.took";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private volatile GraphClientService clientService;
    private volatile GraphQueryGeneratorService graphQueryGeneratorService;
    private volatile RecordReaderFactory recordReaderFactory;
    private volatile RecordSetWriterFactory recordSetWriterFactory;

    private volatile RecordPathCache recordPathCache;

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

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(GraphClientService.class);
        graphQueryGeneratorService = context.getProperty(QUERY_GENERATOR_SERVICE).asControllerService(GraphQueryGeneratorService.class);
        recordReaderFactory = context.getProperty(READER_SERVICE).asControllerService(RecordReaderFactory.class);
        recordSetWriterFactory = context.getProperty(WRITER_SERVICE).asControllerService(RecordSetWriterFactory.class);

        recordPathCache = new RecordPathCache(100);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final String identifierRecordPathValue = context.getProperty(IDENTIFIER_FIELD).evaluateAttributeExpressions(input).getValue();
        final RecordPath identifierRecordPath = recordPathCache.getCompiled(identifierRecordPathValue);
        final String elementLabel = context.getProperty(ELEMENT_LABEL).evaluateAttributeExpressions(input).getValue();
        final GraphElementType elementType = GraphElementType.valueOf(context.getProperty(ELEMENT_TYPE).getValue());

        final Map<String, RecordPath> dynamicPropertyRecordPaths = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> configuredProperty : context.getProperties().entrySet()) {
            final PropertyDescriptor propertyDescriptor = configuredProperty.getKey();
            if (propertyDescriptor.isDynamic()) {
                final String recordPathValue = context.getProperty(propertyDescriptor).evaluateAttributeExpressions(input).getValue();
                dynamicPropertyRecordPaths.put(propertyDescriptor.getName(), recordPathCache.getCompiled(recordPathValue));
            }
        }

        Duration graphOperationsDuration = Duration.ZERO;
        int successfulRecordCount = 0;
        final AtomicBoolean wroteGraphResponse = new AtomicBoolean(false);
        WriteResult failedWriteResult;

        FlowFile failedRecords = session.create(input);
        FlowFile graphResponse = session.create(input);
        try (
                InputStream inputStream = session.read(input);
                RecordReader recordReader = recordReaderFactory.createRecordReader(input, inputStream, getLogger());

                OutputStream failedOutputStream = session.write(failedRecords);
                RecordSetWriter failedWriter = recordSetWriterFactory.createWriter(getLogger(), recordReader.getSchema(), failedOutputStream, input.getAttributes());

                OutputStream graphOutputStream = session.write(graphResponse)
        ) {
            final long processingStartNanos = System.nanoTime();

            failedWriter.beginRecordSet();

            graphOutputStream.write("[".getBytes(StandardCharsets.UTF_8));

            int recordIndex = 0;
            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                try {
                    final List<FieldValue> identifierFieldValues = getFieldValues(record, identifierRecordPath);
                    if (identifierFieldValues.isEmpty()) {
                        throw new IOException("Identifier field(s) not found in record, check the RecordPath expression");
                    }

                    final LinkedHashMap<String, Object> identifierFieldNameToValue = new LinkedHashMap<>(identifierFieldValues.size());
                    final Set<String> identifierFieldNames = new HashSet<>();
                    for (final FieldValue identifierFieldValue : identifierFieldValues) {
                        final Object identifierValue = identifierFieldValue.getValue();
                        if (identifierValue == null) {
                            throw new IOException(String.format("Identifier field '%s' is null for record at index %d", identifierRecordPathValue, recordIndex));
                        }

                        final String identifierFieldName = identifierFieldValue.getField().getFieldName();
                        if (identifierFieldNameToValue.containsKey(identifierFieldName)) {
                            throw new IOException(String.format("Duplicate identifier field '%s' found for record at index %d", identifierFieldName, recordIndex));
                        }
                        identifierFieldNames.add(identifierFieldName);
                        identifierFieldNameToValue.put(identifierFieldName, identifierValue);
                    }

                    final Map<String, Object> propertiesToUpdate = getPropertiesToUpdate(record, identifierFieldNames, dynamicPropertyRecordPaths);
                    final GraphMutation graphMutation = graphQueryGeneratorService.generateSetPropertiesMutation(elementType, identifierFieldNameToValue, elementLabel, propertiesToUpdate);

                    final long queryStartNanos = System.nanoTime();
                    try {
                        clientService.executeQuery(graphMutation.getQuery(), graphMutation.getParameters(), (resultMap, hasMore) -> {
                            try {
                                if (wroteGraphResponse.get()) {
                                    graphOutputStream.write(",".getBytes(StandardCharsets.UTF_8));
                                }

                                graphOutputStream.write(objectMapper.writeValueAsBytes(resultMap));
                                wroteGraphResponse.set(true);
                            } catch (final IOException ioException) {
                                throw new ProcessException("Failed to write graph response", ioException);
                            }
                        });
                    } finally {
                        graphOperationsDuration = graphOperationsDuration.plusNanos(System.nanoTime() - queryStartNanos);
                    }

                    successfulRecordCount++;
                } catch (final GraphClientTransientException transientException) {
                    throw transientException;
                } catch (final Exception e) {
                    getLogger().error("Failed to process record at index {}", recordIndex, e);
                    failedWriter.write(record);
                } finally {
                    recordIndex++;
                }
            }

            graphOutputStream.write("]".getBytes(StandardCharsets.UTF_8));
            final Duration totalProcessingDuration = Duration.ofNanos(System.nanoTime() - processingStartNanos);
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Handled {} records in {} ms, including {} ms spent executing graph queries",
                        successfulRecordCount,
                        totalProcessingDuration.toMillis(),
                        graphOperationsDuration.toMillis());
            }

            failedWriteResult = failedWriter.finishRecordSet();
            failedWriter.flush();
        } catch (final GraphClientTransientException transientException) {
            getLogger().error("Transient graph client failure, rolling back session for retry", transientException);
            context.yield();
            throw transientException;
        } catch (final Exception ex) {
            getLogger().error("Error reading records, routing input FlowFile to failure", ex);
            session.remove(failedRecords);
            session.remove(graphResponse);
            session.transfer(input, FAILURE);
            context.yield();
            return;
        }

        if (successfulRecordCount > 0) {
            graphResponse = session.putAttribute(graphResponse, CoreAttributes.MIME_TYPE.key(), "application/json");
            session.transfer(graphResponse, RESPONSE);
        } else {
            session.remove(graphResponse);
        }

        session.getProvenanceReporter().send(input, clientService.getTransitUrl(), graphOperationsDuration.toMillis());

        if (failedWriteResult != null && failedWriteResult.getRecordCount() < 1) {
            session.remove(failedRecords);
            input = session.putAttribute(input, GRAPH_OPERATIONS_TIME_SECONDS, String.valueOf(graphOperationsDuration.toSeconds()));
            session.transfer(input, ORIGINAL);
        } else if (failedWriteResult != null) {
            failedRecords = session.putAttribute(failedRecords, RECORD_COUNT, String.valueOf(failedWriteResult.getRecordCount()));
            session.transfer(failedRecords, FAILURE);
            session.remove(input);
        }
    }

    private List<FieldValue> getFieldValues(final Record record, final RecordPath recordPath) {
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> values = result.getSelectedFields().toList();
        return values;
    }

    private Map<String, Object> getPropertiesToUpdate(final Record record, final Set<String> identifierFieldNames,
                                                      final Map<String, RecordPath> dynamicPropertyRecordPaths) throws IOException {
        final Map<String, Object> propertiesToUpdate = new HashMap<>();
        if (dynamicPropertyRecordPaths.isEmpty()) {
            final List<String> fieldNames = record.getSchema().getFieldNames();
            for (final String fieldName : fieldNames) {
                if (identifierFieldNames.contains(fieldName)) {
                    continue;
                }

                final List<FieldValue> fieldValues = getFieldValues(record, recordPathCache.getCompiled("/" + fieldName));
                if (fieldValues.isEmpty()) {
                    continue;
                }

                final FieldValue selectedValue = fieldValues.getFirst();
                final Object rawValue = selectedValue.getValue();
                if (rawValue == null) {
                    continue;
                }

                propertiesToUpdate.put(fieldName, normalizeValue(rawValue, selectedValue.getField().getDataType()));
            }
        } else {
            for (final Map.Entry<String, RecordPath> dynamicPropertyRecordPath : dynamicPropertyRecordPaths.entrySet()) {
                final List<FieldValue> fieldValues = getFieldValues(record, dynamicPropertyRecordPath.getValue());
                if (fieldValues.isEmpty() || fieldValues.getFirst().getValue() == null) {
                    throw new IOException("Dynamic property field(s) not found in record, check the RecordPath expression");
                }

                final FieldValue selectedValue = fieldValues.getFirst();
                propertiesToUpdate.put(dynamicPropertyRecordPath.getKey(), normalizeValue(selectedValue.getValue(), selectedValue.getField().getDataType()));
            }
        }

        return propertiesToUpdate;
    }

    private Object normalizeValue(final Object rawValue, final DataType rawDataType) {
        final RecordFieldType rawFieldType = rawDataType.getFieldType();
        if (RecordFieldType.ARRAY.equals(rawFieldType)) {
            final DataType arrayElementDataType = ((ArrayDataType) rawDataType).getElementType();
            if (RecordFieldType.RECORD.getDataType().equals(arrayElementDataType)) {
                final Object[] rawValueArray = (Object[]) rawValue;
                final Object[] mappedValueArray = new Object[rawValueArray.length];
                for (int index = 0; index < rawValueArray.length; index++) {
                    final MapRecord mapRecord = (MapRecord) rawValueArray[index];
                    mappedValueArray[index] = mapRecord.toMap(true);
                }
                return mappedValueArray;
            }

            return rawValue;
        }

        if (RecordFieldType.RECORD.equals(rawFieldType)) {
            final MapRecord mapRecord = (MapRecord) rawValue;
            return mapRecord.toMap(true);
        }

        return rawValue;
    }
}
