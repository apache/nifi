/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"json", "flatten"})
@CapabilityDescription(
        "Provides the user with the ability to take a nested JSON document and flatten it into a simple key/value pair " +
                "document. The keys are combined at each level with a user-defined separator that defaults to '.'. " +
                "Support three kinds of flatten mode, normal, keep-arrays and dot notation for MongoDB query. " +
                "Default flatten mode is 'keep-arrays'."
)
@SideEffectFree
public class FlattenJson extends AbstractProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("Successfully flattened files go to this relationship.")
            .name("success")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .description("Files that cannot be flattened go to this relationship.")
            .name("failure")
            .build();

    static final PropertyDescriptor SEPARATOR = new PropertyDescriptor.Builder()
            .name("flatten-json-separator")
            .displayName("Separator")
            .defaultValue(".")
            .description("The separator character used for joining keys. Must be a JSON-legal character.")
            .addValidator((subject, input, context) -> {
                if (context.isExpressionLanguagePresent(input)) {
                    ExpressionLanguageCompiler elc = context.newExpressionLanguageCompiler();
                    final boolean validExpression = elc.isValidExpression(input);
                    return new ValidationResult.Builder().subject(subject).input(input)
                            .valid(validExpression).explanation(validExpression ? "": "Not a valid Expression").build();
                }

                boolean valid = input != null && input.length() == 1;
                String message = !valid ? "The separator must be a single character in length." : "";

                ObjectMapper mapper = new ObjectMapper();
                String test = String.format("{ \"prop%sprop\": \"test\" }", input);
                try {
                    mapper.readValue(test, Map.class);
                } catch (IOException e) {
                    message = e.getLocalizedMessage();
                    valid = false;
                }

                return new ValidationResult.Builder().subject(subject).input(input).valid(valid).explanation(message).build();
            })
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final AllowableValue FLATTEN_MODE_NORMAL = new AllowableValue("normal", "normal",
            "Flattens every objects into a single level json");

    public static final AllowableValue FLATTEN_MODE_KEEP_ARRAYS = new AllowableValue("keep arrays", "keep arrays",
            "Flattens every objects and keep arrays format");

    public static final AllowableValue FLATTEN_MODE_DOT_NOTATION = new AllowableValue("dot notation", "dot notation",
            "Conforms to MongoDB dot notation to update also nested documents");

    public static final PropertyDescriptor FLATTEN_MODE = new PropertyDescriptor.Builder()
            .name("flatten-mode")
            .displayName("Flatten Mode")
            .description("Specifies how json is flattened")
            .defaultValue(FLATTEN_MODE_KEEP_ARRAYS.getValue())
            .required(true)
            .allowableValues(FLATTEN_MODE_NORMAL, FLATTEN_MODE_KEEP_ARRAYS, FLATTEN_MODE_DOT_NOTATION)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SEPARATOR);
        props.add(FLATTEN_MODE);
        properties = Collections.unmodifiableList(props);

        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);

        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String mode = context.getProperty(FLATTEN_MODE).getValue();
        final FlattenMode flattenMode = getFlattenMode(mode);

        String separator = context.getProperty(SEPARATOR).evaluateAttributeExpressions(flowFile).getValue();


        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            session.exportTo(flowFile, bos);
            bos.close();

            String raw = new String(bos.toByteArray());
            final String flattened = new JsonFlattener(raw)
                    .withFlattenMode(flattenMode)
                    .withSeparator(separator.charAt(0))
                    .withStringEscapePolicy(() -> StringEscapeUtils.ESCAPE_JSON)
                    .flatten();

            flowFile = session.write(flowFile, os -> os.write(flattened.getBytes()));

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception ex) {
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private FlattenMode getFlattenMode(String mode) {
        if (FLATTEN_MODE_NORMAL.getValue().equals(mode)) {
            return FlattenMode.NORMAL;
        } else if (FLATTEN_MODE_DOT_NOTATION.getValue().equals(mode)) {
            return FlattenMode.MONGODB;
        } else {
            return FlattenMode.KEEP_ARRAYS;
        }
    }
}
