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
package org.apache.nifi.processors.deeplearning4j;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"deeplearning4j", "dl4j", "multilayer", "predict", "classification", "regression", "deep", "learning", "neural", "network"})
@CapabilityDescription("The DeepLearning4JMultiLayerPredictor predicts one or more value(s) based on provided deeplearning4j (https://github.com/deeplearning4j) model and the content of a FlowFile. "
    + "The processor supports both classification and regression by extracting the record from the FlowFile body and applying the model. "
    + "The processor supports batch by allowing multiple records to be passed in the FlowFile body with each record separated by the 'Record Separator' property. "
    + "Each record can contain multiple fields with each field separated by the 'Field Separator' property."
    )
@WritesAttributes({
    @WritesAttribute(attribute = AbstractDeepLearning4JProcessor.DEEPLEARNING4J_ERROR_MESSAGE, description = "Deeplearning4J error message"),
    @WritesAttribute(attribute = AbstractDeepLearning4JProcessor.DEEPLEARNING4J_OUTPUT_SHAPE, description = "Deeplearning4J output shape"),
    })
public class DeepLearning4JMultiLayerPredictor extends AbstractDeepLearning4JProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful DeepLearning4j results are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed DeepLearning4j results are routed to this relationship").build();

    protected final Gson gson = new Gson();

    protected MultiLayerNetwork model = null;

    @OnStopped
    public void close() {
        getLogger().info("Closing");
        model = null;
    }

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;
    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(tempRelationships);
        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(MODEL_FILE);
        tempDescriptors.add(RECORD_DIMENSIONS);
        tempDescriptors.add(CHARSET);
        tempDescriptors.add(FIELD_SEPARATOR);
        tempDescriptors.add(RECORD_SEPARATOR);
        propertyDescriptors = Collections.unmodifiableList(tempDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    protected synchronized MultiLayerNetwork getModel(ProcessContext context) throws IOException {
        if ( model == null ) {
            String modelFile = context.getProperty(MODEL_FILE).evaluateAttributeExpressions().getValue();
            getLogger().debug("Loading model from {}", new Object[] {modelFile});

            long start = System.currentTimeMillis();
            model = ModelSerializer.restoreMultiLayerNetwork(modelFile,false);
            long end = System.currentTimeMillis();

            getLogger().info("Time to load model " + (end-start) +  " ms");
        }
        return (MultiLayerNetwork)model;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        if ( flowFile.getSize() == 0 ) {
            String message = "FlowFile query is empty";
            getLogger().error(message);
            flowFile = session.putAttribute(flowFile, DEEPLEARNING4J_ERROR_MESSAGE, message);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        String input = null;
        try {
            input = getFlowFileContents(session, charset, flowFile);
            String fieldSeparator = context.getProperty(FIELD_SEPARATOR).evaluateAttributeExpressions(flowFile).getValue();
            String recordSeparator = context.getProperty(RECORD_SEPARATOR).evaluateAttributeExpressions(flowFile).getValue();

            int [] dimensions = getInputDimensions(context, charset, flowFile, fieldSeparator);

            if ( getLogger().isDebugEnabled() )    {
                getLogger().debug("Received input {} with dimensions {}", new Object[] { input, dimensions });
            }

            MultiLayerNetwork model = getModel(context);

            long startTimeMillis = System.currentTimeMillis();

            String [] inputRecords = input.split(recordSeparator);

            List<INDArray> features = Arrays.stream(inputRecords).map(
                record -> {
                    double [] parameters = Arrays.stream(record.split(fieldSeparator)).mapToDouble(
                             field -> Double.parseDouble(field)).toArray();

                    INDArray featureInput = Nd4j.create(parameters, dimensions);

                    if ( getLogger().isDebugEnabled() ) {
                        getLogger().debug("Features for record {} parameters {} dims {} featureInput {} ",
                            new Object[] {record, parameters, dimensions, featureInput});
                    }

                    return featureInput;

                }).collect(Collectors.toList());

           INDArray allFeatures = Nd4j.vstack(features);

           INDArray results = model.output(allFeatures);

           double [][] partitionedResults = new double[inputRecords.length][];
           for (int row = 0; row < inputRecords.length; row++) {
                INDArray result = results.getRow(row);
                if (result.isScalar()) {
                    partitionedResults[row] = new double[]{result.getDouble(0)};
                } else {
                    partitionedResults[row] = Nd4j.toFlattened(result).toDoubleVector();
                }
           }

           String jsonResult = gson.toJson(partitionedResults);
           int [] shape = results.shape();
           String jsonShape = gson.toJson(Arrays.copyOfRange(shape, 1, shape.length));

           if ( getLogger().isDebugEnabled() ) {
                getLogger().debug("Prediction for inputRecords {}, dims {}, results {}, result.shape {}, partitionedResults {}, jsonResult {}, shape {}, jsonShape {}",
                        new Object[] {inputRecords, dimensions, results, Arrays.toString(results.shape()), partitionedResults, jsonResult, shape, jsonShape});
           }

           flowFile = session.write(flowFile, out -> out.write(jsonResult.getBytes(charset)));

           session.putAttribute(flowFile, DEEPLEARNING4J_OUTPUT_SHAPE, jsonShape);

           final long endTimeMillis = System.currentTimeMillis();

           session.transfer(flowFile, REL_SUCCESS);

           session.getProvenanceReporter().modifyContent(flowFile, makeProvenanceUrl(context),
                    (endTimeMillis - startTimeMillis));
        } catch (Exception exception) {
            flowFile = session.putAttribute(flowFile, DEEPLEARNING4J_ERROR_MESSAGE, String.valueOf(exception.getMessage()));
            getLogger().error("Failed to process data due to {} for input {}",
                    new Object[]{exception.getLocalizedMessage(), input}, exception);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    protected String getFlowFileContents(final ProcessSession session, Charset charset, FlowFile incomingFlowFile)
            throws IOException {
        final byte[] buffer = new byte[(int) incomingFlowFile.getSize()];
        try (final InputStream in = session.read(incomingFlowFile)) {
            StreamUtils.fillBuffer(in, buffer);
        }
        return new String(buffer, charset);
    }

    protected int [] getInputDimensions(final ProcessContext context, Charset charset, FlowFile flowFile, String separator)
            throws IOException {
        String values = context.getProperty(RECORD_DIMENSIONS).evaluateAttributeExpressions(flowFile).getValue();
        return Arrays.stream(
                values.split(separator))
                    .mapToInt(val -> Integer.parseInt(val)).toArray();
    }

    protected String makeProvenanceUrl(final ProcessContext context) {
        return new StringBuilder("deeplearning4j://")
            .append(context.getProperty(MODEL_FILE).evaluateAttributeExpressions().getValue()).toString();
    }

    protected FlowFile populateErrorAttributes(final ProcessSession session, FlowFile flowFile,
            String message) {
        Map<String,String> attributes = new HashMap<>();
        attributes.put(DEEPLEARNING4J_ERROR_MESSAGE, String.valueOf(message));
        flowFile = session.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

}