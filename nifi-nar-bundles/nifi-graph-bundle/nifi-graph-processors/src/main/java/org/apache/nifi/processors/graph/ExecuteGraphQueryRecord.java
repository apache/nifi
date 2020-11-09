package org.apache.nifi.processors.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Tags({"graph, gremlin"})
@CapabilityDescription("This uses a flowfile as input to perform graph mutations.")
@WritesAttributes({
        @WritesAttribute(attribute = ExecuteGraphQueryRecord.GRAPH_OPERATION_TIME, description = "The amount of time it took to execute all of the graph operations."),
        @WritesAttribute(attribute = ExecuteGraphQueryRecord.RECORD_COUNT, description = "The amount of record processed")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "A FlowFile property to be used as a parameter in the graph script",
        value = "The variable name to be set", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Uses a record path to set a variable as a parameter in the graph script")
public class ExecuteGraphQueryRecord extends  AbstractGraphExecutor {

    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("client-service")
            .displayName("Client Service")
            .description("The graph client service for connecting to a graph database.")
            .identifiesControllerService(GraphClientService.class)
            .addValidator(Validator.VALID)
            .required(true)
            .build();

    public static final PropertyDescriptor READER_SERVICE = new PropertyDescriptor.Builder()
            .name("reader-service")
            .displayName("Record Reader")
            .description("The record reader to use with this processor.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor WRITER_SERVICE = new PropertyDescriptor.Builder()
            .name("writer-service")
            .displayName("Failed Record Writer")
            .description("The record writer to use for writing failed records.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor SUBMISSION_SCRIPT = new PropertyDescriptor.Builder()
            .name("record-script")
            .displayName("Graph Record Script")
            .description("Script to perform the business logic on graph, using flow file attributes and custom properties " +
                    "as variable-value pairs in its logic.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    }

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            CLIENT_SERVICE, READER_SERVICE, WRITER_SERVICE, SUBMISSION_SCRIPT
    ));

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("original")
                                                    .description("The original flowfile")
                                                    .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
                                                    .description("Flow files that fail to interact with graph server")
                                                    .build();
    public static final Relationship REL_ERRORS = new Relationship.Builder().name("errors")
                                                    .description("Flow files that error in the response from graph server")
                                                    .build();
    public static final Relationship REL_GRAPH = new Relationship.Builder().name("response")
                                                    .description("The response object from the graph server")
                                                    .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS, REL_FAILURE, REL_ERRORS, REL_GRAPH
    )));

    public static final String RECORD_COUNT = "records.count";
    public static final String GRAPH_OPERATION_TIME = "graph.operations.took";
    private volatile RecordPathCache recordPathCache;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private GraphClientService clientService;
    private RecordReaderFactory recordReaderFactory;
    private RecordSetWriterFactory recordSetWriterFactory;
    private ObjectMapper mapper = new ObjectMapper();

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(GraphClientService.class);
        recordReaderFactory = context.getProperty(READER_SERVICE).asControllerService(RecordReaderFactory.class);
        recordSetWriterFactory = context.getProperty(WRITER_SERVICE).asControllerService(RecordSetWriterFactory.class);
        recordPathCache = new RecordPathCache(100);
    }

    private List<Object> getRecordValue(ProcessContext context, PropertyDescriptor entry, Record record, FlowFile input){
        String valueRecordPath = context.getProperty(entry.getName()).evaluateAttributeExpressions(input).getValue();
        final RecordPath recordPath = recordPathCache.getCompiled(valueRecordPath);
        final RecordPathResult result = recordPath.evaluate(record);
        return result.getSelectedFields()
                .filter(fv -> fv.getValue() != null)
                .map(FieldValue::getValue)
                .collect( Collectors.toList());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if ( input == null ) {
            return;
        }

        FlowFile output = session.create(input);
        FlowFile graph = session.create(input);
        session.getProvenanceReporter().clone(input, graph);

        String recordScript = context.getProperty(SUBMISSION_SCRIPT)
                .evaluateAttributeExpressions(input)
                .getValue();

        boolean failed = false;
        boolean error = false;
        final AtomicLong errors = new AtomicLong();
        long delta = 0;
        Map<String, String> attributes = new HashMap<>();
        List<Map<String,Object>> graphResponses = new ArrayList<>();
        try (InputStream is = session.read(input);
             OutputStream os = session.write(output);
             OutputStream graphOutputStream = session.write(graph);
             RecordReader reader = recordReaderFactory.createRecordReader(attributes, is, -1l, getLogger());
             RecordSetWriter writer = recordSetWriterFactory.createWriter(getLogger(), reader.getSchema(), os, input.getAttributes());
        ) {
            List<PropertyDescriptor> dynamic = context.getProperties()
                    .keySet().stream()
                    .filter(PropertyDescriptor::isDynamic)
                    .collect(Collectors.toList());
            Record record;

            long start = System.currentTimeMillis();
            long recordIndex = 0;
            long batchStart = start;
            writer.beginRecordSet();
            while ((record = reader.nextRecord()) != null) {
                Map<String, Object> dynamicPropertyMap = new HashMap<>();
                for (PropertyDescriptor entry : dynamic) {
                        if(!dynamicPropertyMap.containsKey(entry.getName())) {
                            dynamicPropertyMap.put(entry.getName(), getRecordValue(context, entry, record, input));
                        }
                }
                dynamicPropertyMap.putAll(input.getAttributes());
                graphResponses.addAll(executeQuery(recordScript, dynamicPropertyMap));
                recordIndex++;
                if (recordIndex % 100 == 0) {
                    long now = System.currentTimeMillis();
                    batchStart = System.currentTimeMillis();
                }
            }
            try {
                String graphOutput = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(graphResponses);
                graphOutputStream.write(graphOutput.getBytes(StandardCharsets.UTF_8));
                session.getProvenanceReporter().modifyContent(graph);
            } catch ( Exception ex ) {
                getLogger().error("", ex);
                error = true;
            }
            writer.finishRecordSet();
            long end = System.currentTimeMillis();
            delta = (end - start) / 1000;
            if (getLogger().isDebugEnabled()){
                getLogger().debug(String.format("Took %s seconds.", delta));
            }
        } catch (Exception ex) {
            getLogger().error("", ex);
            failed = true;
        } finally {
            if (failed) {
                session.remove(output);
                session.getProvenanceReporter().route(input, REL_FAILURE);
                session.transfer(input, REL_FAILURE);
            } else {
                input = session.putAttribute(input, GRAPH_OPERATION_TIME, String.valueOf(delta));
                session.getProvenanceReporter().send(input, clientService.getTransitUrl(), delta*1000);
                session.transfer(input, REL_SUCCESS);
                if (!error) {
                    session.getProvenanceReporter().route(graph, REL_GRAPH);
                    session.transfer(graph, REL_GRAPH);
                } else {
                    session.remove(graph);
                }
                if (errors.get() > 0) {
                    session.getProvenanceReporter().route(output, REL_ERRORS);
                    session.transfer(output, REL_ERRORS);
                } else {
                    session.remove(output);
                }
            }
        }
    }

    private List<Map<String, Object>> executeQuery(String recordScript, Map<String, Object> parameters) {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> graphResponses = new ArrayList<>();
        clientService.executeQuery(recordScript, parameters, (map, b) -> {
            if (getLogger().isDebugEnabled()){
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
