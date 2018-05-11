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
import java.io.IOException;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;

/**
 * Base class for deeplearning4j processors
 */
public abstract class AbstractDeepLearning4JProcessor extends AbstractProcessor {

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("deeplearning4j-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELD_SEPARATOR = new PropertyDescriptor.Builder()
            .name("deeplearning4j-field-separator")
            .displayName("Field Separator")
            .description("Specifies the field separator in the records.")
            .required(true)
            .defaultValue(",")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_SEPARATOR = new PropertyDescriptor.Builder()
            .name("deeplearning4j-record-separator")
            .displayName("Record Separator")
            .description("Specifies the records separator in the message body.")
            .required(true)
            .defaultValue("\n")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODEL_FILE = new PropertyDescriptor.Builder()
            .name("model-file")
            .displayName("Model File")
            .description("Location of the Deeplearning4J model zip file")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_DIMENSIONS = new PropertyDescriptor.Builder()
            .name("deeplearning4j-record-dimension")
            .displayName("Record Dimensions")
            .description("Dimension of array in each a record (eg: 2,4 - a 2x4 array).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final String DEEPLEARNING4J_ERROR_MESSAGE = "deeplearning4j.error.message";

    public static final String DEEPLEARNING4J_OUTPUT_SHAPE = "deeplearning4j.output.shape";

    protected MultiLayerNetwork model = null;

    protected synchronized MultiLayerNetwork getModel(ProcessContext context) throws IOException {
        if ( model == null ) {
            String modelFile = context.getProperty(MODEL_FILE).evaluateAttributeExpressions().getValue();
            getLogger().debug("Loading model from {}", new Object[] {modelFile});

            long start = System.currentTimeMillis();
            model = ModelSerializer.restoreMultiLayerNetwork(modelFile,false);
            long end = System.currentTimeMillis();

            getLogger().info("Time to load model " + (end-start) +  " ms");
        }
        return model;
    }

    @OnStopped
    public void close() {
        getLogger().info("Closing");
        model = null;
    }
}
