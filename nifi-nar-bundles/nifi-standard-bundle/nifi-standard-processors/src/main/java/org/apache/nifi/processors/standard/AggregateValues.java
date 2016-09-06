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
package org.apache.nifi.processors.standard;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import net.sf.saxon.lib.NamespaceConstant;
import net.sf.saxon.xpath.XPathEvaluator;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.xml.sax.InputSource;

import javax.xml.transform.Source;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static javax.xml.xpath.XPathConstants.NODESET;

/**
 * A processor to intermediateAggregate values from incoming flow files. The flow files are expected to have certain attributes set to allow for
 * aggregations over a batch of flow files.
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "xml", "csv", "aggregate"})
@SeeAlso({SplitText.class, SplitJson.class, SplitXml.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Aggregates values from incoming flow files based on a path to the desired field and an aggregation operation (SUM, AVG, etc.). "
        + "The flow files are expected to have mime.type, fragment.count, fragment.identifier and fragment.index attributes set, in order for the processor to determine "
        + "when a fragment/batch is complete. Supported MIME types include text/csv, text/plain, application/json, and application/xml. For text/csv and text/plain types, "
        + "the flow file is expected to contain a single row of comma-delimited data (no headers). SplitText can be used for this purpose. For JSON files, SplitJson can be "
        + "used to split the document (setting the expected attributes, etc.). For XML files, SplitXml can be used in a similar manner."
        + "Note that all flow files with the same identifier must be routed to this processor, otherwise the count will need "
        + "need to be decremented for each flow file in order to represent the number of flow files with the same identifier that will be routed to this processor. "
        + "Once all fragments with the same identifier have been processed, a flow file will be transferred to the 'aggregate' relationship containing an attribute with the "
        + "aggregated value")
@Stateful(scopes = Scope.LOCAL, description = "The processor will temporarily keep state information for each fragment.identifier, including the current intermediateAggregate "
        + "value, the fragment count, and the current number of fragments processed. Once all fragments have been processed, the state information for that fragment "
        + "will be cleared.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "mime.type", description = "The MIME type of the flow file content."),
        @ReadsAttribute(attribute = "fragment.identifier", description = "The unique identifier for a collection of flow files."),
        @ReadsAttribute(attribute = "fragment.count", description = "The total number of incoming flow files with the unique identifier."),
        @ReadsAttribute(attribute = "fragment.index", description = "The index of this flow file into the collection of flow files with the same identifier.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "aggregate.value", description = "The aggregated value from applying the specified operation to each flow file with the same identifier."),
        @WritesAttribute(attribute = "aggregate.operation", description = "The name of the aggregation operation applied to the field in the flow files."),
        @WritesAttribute(attribute = "fragment.identifier", description = "The unique identifier for the flow files used in the aggregation."),
        @WritesAttribute(attribute = "fragment.count", description = "The total number of incoming flow files with the unique identifier."),

})
public class AggregateValues extends AbstractSessionFactoryProcessor {

    public static final String FRAGMENT_ID = "fragment.identifier";
    public static final String FRAGMENT_INDEX = "fragment.index";
    public static final String FRAGMENT_COUNT = "fragment.count";
    public static final String AGGREGATE_VALUE = "aggregate.value";
    public static final String AGGREGATE_OPERATION = "aggregate.operation";

    public static final String CSV_MIME_TYPE = "text/csv";
    public static final String JSON_MIME_TYPE = "application/json";
    public static final String XML_MIME_TYPE = "application/xml";

    public static final String COUNT = "COUNT";
    public static final String SUM = "SUM";
    public static final String AVG = "AVERAGE";
    public static final String MIN = "MIN";
    public static final String MAX = "MAX";
    public static final String CONCAT = "CONCAT";

    private static final Configuration STRICT_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();
    private final AtomicReference<XPathFactory> factoryRef = new AtomicReference<>();

    // Properties
    public static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
            .name("aggregator-path")
            .displayName("Path To Field")
            .description("An expression for selecting a field from the incoming file. For JSON files this will be a JSON Path expression to a field of primitive type, "
                    + "for XML files this will be a XPath expression to a single element, and for CSV files this will be a column name (if a header line is present) "
                    + "or an column index (0-based).")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor OPERATION = new PropertyDescriptor.Builder()
            .name("aggregator-operation")
            .displayName("Operation")
            .description("Which aggregation operation to perform on the specified field for a batch of flow files. Note that some operations can/must take a parameter, "
                    + "which is specified in the Operation Parameter property.")
            .required(true)
            .allowableValues(COUNT, SUM, AVG, MIN, MAX, CONCAT)
            .defaultValue(COUNT)
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPERATION_PARAM = new PropertyDescriptor.Builder()
            .name("aggregator-operation-parameter")
            .displayName("Operation Parameter")
            .description("An optional parameter given to the aggregation operation. For COUNT, this is an optional integer value that indicates the number to "
                    + "increment the count by (defaults to 1). For SUM, MAX, MIN, AVERAGE, the value is ignored. For CONCAT, the value is a string inserted "
                    + "between each field's value in the intermediateAggregate string.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    // Relationships
    public static final Relationship REL_AGGREGATE = new Relationship.Builder()
            .name("aggregate")
            .description("Successfully created FlowFile with an attribute for the aggregated value.")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Once an incoming flow file is processed successfully, it will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not in a valid format or the specified "
                    + "path does not exist), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    static {
        System.setProperty("javax.xml.xpath.XPathFactory:" + NamespaceConstant.OBJECT_MODEL_SAXON, "net.sf.saxon.xpath.XPathFactoryImpl");
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PATH);
        properties.add(OPERATION);
        properties.add(OPERATION_PARAM);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_AGGREGATE);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void initializeXPathFactory() throws XPathFactoryConfigurationException {
        factoryRef.set(XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        final ComponentLog logger = getLogger();
        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        try {
            stateMap = stateManager.getState(Scope.LOCAL);
        } catch (final IOException ioe) {
            logger.error("Failed to retrieve intermediate intermediateAggregate values from the State Manager. Will not perform "
                    + "aggregation until this is accomplished.", ioe);
            context.yield();
            return;
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Make a mutable copy of the current state property map. This will be updated with new intermediate aggregation
        // results, and eventually set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        try {
            final String fragmentIdentifier = flowFile.getAttribute(FRAGMENT_ID);
            if (StringUtils.isEmpty(fragmentIdentifier)) {
                throw new IOException("Flow File " + flowFile + "has empty or missing fragment.identifier attribute");
            }
            final String fragmentCount = flowFile.getAttribute(FRAGMENT_COUNT);
            if (StringUtils.isEmpty(fragmentCount)) {
                throw new IOException("Flow File " + flowFile + "has empty or missing fragment.count attribute");
            }
            final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
            if (StringUtils.isEmpty(mimeType)) {
                throw new IOException("Flow File " + flowFile + "has empty or missing mime.type attribute");
            }
            final String fieldPath = context.getProperty(PATH).getValue();
            final String operation = context.getProperty(OPERATION).getValue();
            final String operationParam = context.getProperty(OPERATION_PARAM).evaluateAttributeExpressions(flowFile).getValue();

            Object fieldValue;

            switch (mimeType) {
                case CSV_MIME_TYPE:
                    fieldValue = getValueFromCSV(session, flowFile, fieldPath);
                    break;
                case JSON_MIME_TYPE:
                    fieldValue = getValueFromJSON(session, flowFile, fieldPath);
                    break;
                case XML_MIME_TYPE:
                    fieldValue = getValueFromXML(session, flowFile, fieldPath);
                    break;
                default:
                    throw new IOException("MIME type not supported by AggregateValues: " + mimeType);
            }

            // Get state property names
            final String valueProperty = fragmentIdentifier + ".value";
            final String countProperty = fragmentIdentifier + ".count";
            final String typeProperty = fragmentIdentifier + ".type";

            // Get current (intermediate) value of aggregation, etc. from state
            String intermediateValue = statePropertyMap.get(valueProperty);
            String intermediateCountString = statePropertyMap.get(countProperty);
            int intermediateCount = StringUtils.isEmpty(intermediateCountString) ? 0 : Integer.parseInt(intermediateCountString);
            String intermediateType = statePropertyMap.get(typeProperty);
            if (StringUtils.isEmpty(intermediateType)) {
                if (fieldValue instanceof Integer) {
                    intermediateType = "int";
                } else if (fieldValue instanceof Float) {
                    intermediateType = "float";
                } else if (fieldValue instanceof Double) {
                    intermediateType = "double";
                } else if (fieldValue instanceof String) {
                    intermediateType = "string";
                } else if (fieldValue instanceof Boolean) {
                    intermediateType = "boolean";
                }
            }

            // Perform operation
            Object newValue = intermediateAggregate(fieldValue, intermediateValue, intermediateType, operation, operationParam);

            // Check for complete, clear state if so
            intermediateCount++;
            final int total = Integer.parseInt(fragmentCount);
            if (intermediateCount < total) {
                statePropertyMap.put(valueProperty, newValue.toString());
                statePropertyMap.put(countProperty, Integer.toString(intermediateCount));
            } else {
                // If average, divide sum by total
                if (AVG.equals(operation)) {
                    if ("int" .equals(intermediateType)) {
                        newValue = ((Integer) newValue) / ((float) total);
                    } else if ("float" .equals(intermediateType)) {
                        newValue = ((Float) newValue) / ((float) total);
                    } else if ("double" .equals(intermediateType)) {
                        newValue = ((Double) newValue) / ((double) total);
                    } else {
                        throw new IllegalArgumentException("Fields of type " + intermediateType + " cannot be used for AVG aggregations");
                    }
                }
                statePropertyMap.remove(valueProperty);
                statePropertyMap.remove(countProperty);

                FlowFile aggregateFlowFile = session.create();
                aggregateFlowFile = session.putAttribute(aggregateFlowFile, AGGREGATE_VALUE, newValue.toString());
                aggregateFlowFile = session.putAttribute(aggregateFlowFile, AGGREGATE_OPERATION, operation);
                aggregateFlowFile = session.putAttribute(aggregateFlowFile, FRAGMENT_ID, fragmentIdentifier);
                aggregateFlowFile = session.putAttribute(aggregateFlowFile, FRAGMENT_COUNT, fragmentCount);

                session.transfer(aggregateFlowFile, REL_AGGREGATE);
            }

            session.transfer(flowFile, REL_ORIGINAL);

        } catch (IllegalArgumentException | IOException e) {
            // Something bad happened while reading the file, log the error and send to failure
            logger.error("Error processing FlowFile {} due to {}", new Object[]{flowFile, e.getMessage()}, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            session.commit();
            try {
                // Update the state
                stateManager.setState(statePropertyMap, Scope.LOCAL);
            } catch (IOException ioe) {
                logger.error("{} failed to update State Manager, intermediateAggregate results may be incorrect",
                        new Object[]{this, ioe});
            }
        }
    }

    protected Object intermediateAggregate(Object fieldValue, String intermediateValue, String intermediateValueType, String operation, String operationParam) {
        Object result = null;

        switch (operation) {
            case COUNT:
                // Expects intermediate value and operationParam to be integers
                result = (StringUtils.isEmpty(intermediateValue) ? 0 : Integer.parseInt(intermediateValue))
                        + (StringUtils.isEmpty(operationParam) ? 1 : Integer.parseInt(operationParam));
                break;

            case SUM:
            case AVG: // Average for now is just sum
                if ("int" .equals(intermediateValueType)) {
                    result = (StringUtils.isEmpty(intermediateValue) ? 0 : Integer.parseInt(intermediateValue))
                            + ((Integer) fieldValue);
                } else if ("float" .equals(intermediateValueType)) {
                    result = (StringUtils.isEmpty(intermediateValue) ? 0.0f : Float.parseFloat(intermediateValue))
                            + ((Float) fieldValue);
                } else if ("double" .equals(intermediateValueType)) {
                    result = (StringUtils.isEmpty(intermediateValue) ? 0.0 : Double.parseDouble(intermediateValue))
                            + ((Double) fieldValue);
                } else {
                    throw new IllegalArgumentException("Fields of type " + intermediateValueType + " cannot be used for SUM/AVG aggregations");
                }
                break;
            case MAX:
                if (StringUtils.isEmpty(intermediateValue)) {
                    result = fieldValue;
                } else if ("int" .equals(intermediateValueType)) {
                    Integer currMax = Integer.parseInt(intermediateValue);
                    result = ((Integer) fieldValue > currMax) ? fieldValue : currMax;
                } else if ("float" .equals(intermediateValueType)) {
                    Float currMax = Float.parseFloat(intermediateValue);
                    result = ((Float) fieldValue > currMax) ? fieldValue : currMax;
                } else if ("double" .equals(intermediateValueType)) {
                    Double currMax = Double.parseDouble(intermediateValue);
                    result = ((Double) fieldValue > currMax) ? fieldValue : currMax;
                } else if ("string" .equals(intermediateValueType)) {
                    result = ((String) fieldValue).compareTo(intermediateValue) > 0 ? fieldValue : intermediateValue;
                } else {
                    throw new IllegalArgumentException("Fields of type " + intermediateValueType + " cannot be used for MAX aggregations");
                }
                break;
            case MIN:
                if (StringUtils.isEmpty(intermediateValue)) {
                    result = fieldValue;
                } else if ("int" .equals(intermediateValueType)) {
                    Integer currMax = Integer.parseInt(intermediateValue);
                    result = ((Integer) fieldValue < currMax) ? fieldValue : currMax;
                } else if ("float" .equals(intermediateValueType)) {
                    Float currMax = Float.parseFloat(intermediateValue);
                    result = ((Float) fieldValue < currMax) ? fieldValue : currMax;
                } else if ("double" .equals(intermediateValueType)) {
                    Double currMax = Double.parseDouble(intermediateValue);
                    result = ((Double) fieldValue < currMax) ? fieldValue : currMax;
                } else if ("string" .equals(intermediateValueType)) {
                    result = ((String) fieldValue).compareTo(intermediateValue) < 0 ? fieldValue : intermediateValue;
                } else {
                    throw new IllegalArgumentException("Fields of type " + intermediateValueType + " cannot be used for MIN aggregations");
                }
                break;
            case CONCAT:
                if ("string" .equals(intermediateValueType)) {
                    result = (intermediateValue == null ? "" : (intermediateValue + (StringUtils.isEmpty(operationParam) ? "" : operationParam)))
                            + fieldValue;
                } else {
                    throw new IllegalArgumentException("Fields of type " + intermediateValueType + " cannot be used for CONCAT aggregations");
                }
                break;
        }
        return result;
    }

    protected Object getValueFromCSV(ProcessSession session, FlowFile flowFile, String csvExp) throws IOException {
        // The CSV expression is expected to be an integer referring to the column index
        final int colIndex;
        try {
            colIndex = Integer.parseInt(csvExp);
        } catch (NumberFormatException nfe) {
            throw new IOException("CSV expression '" + csvExp + "' is not an integer");
        }

        final AtomicReference<String> csvLine = new AtomicReference<>();

        session.read(flowFile, in -> {
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(in)))) {
                csvLine.set(reader.readLine());
            }
        });

        String line = csvLine.get();
        if (line == null) {
            throw new IOException("Flow File has no CSV content");
        }
        // Split the columns on commas, but use the occurrences of commas for the number of columns
        int numCols = StringUtils.countMatches(line, ",") + 1;
        String[] columns = line.split(",");
        if (colIndex > numCols || colIndex < 0) {
            throw new IOException("Invalid column index: " + csvExp);
        } else if (colIndex == numCols) {
            return null;
        } else {
            Object result = columns[colIndex];
            // If the object can be parsed as a Double, treat it like one, otherwise treat it like a string
            try {
                result = Double.parseDouble(result.toString());
            } catch (NumberFormatException nfe) {
                // Keep the original object
            }
            return result;
        }
    }

    protected Object getValueFromJSON(ProcessSession session, FlowFile flowFile, String jsonPathExp) throws IOException {
        DocumentContext documentContext;
        try {
            documentContext = validateAndEstablishJsonContext(session, flowFile);
        } catch (InvalidJsonException e) {
            throw new IOException("FlowFile " + flowFile + " did not have valid JSON content.", e);
        }

        try {
            final Object result = documentContext.read(jsonPathExp);
            if (!isJsonScalar(result)) {
                throw new IOException("Unable to return a scalar value for the expression "
                        + jsonPathExp + " for FlowFile " + flowFile
                        + ". Evaluated value was " + result.toString() + ".");
            }
            return result;
        } catch (PathNotFoundException e) {
            throw new IOException("Could not find path " + jsonPathExp + " in FlowFile " + flowFile, e);
        }
    }

    protected Object getValueFromXML(ProcessSession session, FlowFile flowFile, String xPathExp) throws IOException {
        Object result;
        final XPathFactory factory = factoryRef.get();
        final XPathEvaluator xpathEvaluator = (XPathEvaluator) factory.newXPath();
        final XPathExpression xpathExpression;
        try {
            xpathExpression = xpathEvaluator.compile(xPathExp);
            final AtomicReference<Throwable> error = new AtomicReference<>(null);
            final AtomicReference<Source> sourceRef = new AtomicReference<>(null);

            final XPathExpression slashExpression;
            try {
                slashExpression = xpathEvaluator.compile("/");
            } catch (XPathExpressionException e) {
                throw new IOException("unable to compile XPath expression due to " + e.getMessage(), e);
            }

            session.read(flowFile, rawIn -> {
                try (final InputStream in = new BufferedInputStream(rawIn)) {
                    final List<Source> rootList = (List<Source>) slashExpression.evaluate(new InputSource(in), NODESET);
                    sourceRef.set(rootList.get(0));
                } catch (final Exception e) {
                    error.set(e);
                }
            });

            if (error.get() != null) {
                throw new IOException("Unable to evaluate XPath against " + flowFile + " due to " + error.get().getMessage(), error.get());
            }

            try {
                if (xpathExpression.evaluate(sourceRef.get(), XPathConstants.NODE) == null) {
                    throw new IOException("Could not find path " + xPathExp + " in FlowFile " + flowFile);
                }
                result = xpathExpression.evaluate(sourceRef.get());

                // If the object can be parsed as a Double, treat it like one, otherwise treat it like a string
                try {
                    result = Double.parseDouble(result.toString());
                } catch (NumberFormatException nfe) {
                    // Keep the original object
                }

            } catch (final XPathExpressionException e) {
                throw new IOException("Failed to evaluate XPath for " + flowFile + " for path " + xpathExpression.toString() + " due to " + e.getMessage(), e);
            }

        } catch (XPathExpressionException e) {
            throw new IOException(e);  // should not happen because we've already validated the XPath (in XPathValidator)
        }
        return result;
    }

    private static DocumentContext validateAndEstablishJsonContext(ProcessSession processSession, FlowFile flowFile) {
        // Parse the document once into an associated context to support multiple path evaluations if specified
        final AtomicReference<DocumentContext> contextHolder = new AtomicReference<>(null);
        processSession.read(flowFile, in -> {
            try (BufferedInputStream bufferedInputStream = new BufferedInputStream(in)) {
                DocumentContext ctx = JsonPath.using(STRICT_PROVIDER_CONFIGURATION).parse(bufferedInputStream);
                contextHolder.set(ctx);
            }
        });

        return contextHolder.get();
    }

    /**
     * Determines the context by which JsonSmartJsonProvider would treat the value. {@link java.util.Map} and {@link java.util.List} objects can be rendered as JSON elements, everything else is
     * treated as a scalar.
     *
     * @param obj item to be inspected if it is a scalar or a JSON element
     * @return false, if the object is a supported type; true otherwise
     */
    private static boolean isJsonScalar(Object obj) {
        // For the default provider, JsonSmartJsonProvider, a Map or List is able to be handled as a JSON entity
        return !(obj instanceof Map || obj instanceof List);
    }

    private static boolean isNumeric(String type) {
        switch (type) {
            case "int":
            case "double":
                return true;
            default:
                return false;
        }
    }
}
