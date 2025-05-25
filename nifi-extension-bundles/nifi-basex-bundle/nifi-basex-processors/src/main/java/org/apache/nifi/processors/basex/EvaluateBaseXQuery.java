/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.basex;



import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.basex.core.Context;
import org.basex.io.IOContent;
import org.basex.query.QueryProcessor;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.node.DBNode;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@SideEffectFree
@Tags({"XML", "evaluate", "XPath", "XQuery", "basex"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "Executes an XQuery expression using the BaseX engine, passing the FlowFile content as a string context to the script. "
                + "An XQuery script must be provided for the processor to run. Users can choose how FlowFile attributes are mapped "
                + "to external variables: map all attributes, map only a specific list, or ignore attributes altogether. "
                + "The output of the query can be written to a specified FlowFile attribute (similarly to the InvokeHTTP processor) "
                + "or used to replace the content of the original FlowFile. The processor defines the following relationships: "
                + "'success' (on successful execution), 'failure' (if the script fails), and 'flowFileCloneOriginal' (preserves the unmodified FlowFile)."
)
@SystemResourceConsideration(
        resource = SystemResource.MEMORY,
        description = "Entire FlowFile content is fully loaded into memory, potentially multiple times during processing and serialization." +
                " This can lead to very high memory usage, especially with large XML documents. Users should avoid large FlowFiles or ensure ample " +
                "heap memory is available."
)
public class EvaluateBaseXQuery extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Content extraction failed").build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("Success for original input FlowFiles").build();

    public static final PropertyDescriptor XQUERY_SCRIPT = new PropertyDescriptor.Builder()
            .name("XQuery Script")
            .description("XQuery to execute on the incoming FlowFile content")
            .required(true)
            .addValidator(new BaseXqueryValidator())
            .build();



    public static final String MAP_ALL = "Map all";
    public static final String MAP_LIST = "Map list";
    public static final String DO_NOT_MAP = "Do not map";

    public static final PropertyDescriptor ATTR_MAPPING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Attribute Mapping Strategy")
            .description("How to map FlowFile attributes as external XQuery variables")
            .allowableValues(MAP_ALL, MAP_LIST, DO_NOT_MAP)
            .required(true)
            .build();

    public static final PropertyDescriptor MAPPING_LIST = new PropertyDescriptor.Builder()
            .name("Mapping list")
            .description("List of attributes to map to external variables, separated by comma")
            .required(false)
            .dependsOn(ATTR_MAPPING_STRATEGY, MAP_LIST)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor OUTPUT_BODY_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Response Body Attribute Name")
            .description("FlowFile attribute name used to write an xquery output body for FlowFiles transferred to the original relationship.")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_ORIGINAL
    );
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(XQUERY_SCRIPT);
        descriptors.add(ATTR_MAPPING_STRATEGY);
        descriptors.add(MAPPING_LIST);
        descriptors.add(OUTPUT_BODY_ATTRIBUTE_NAME);
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        Context basexContext = new Context();
        FlowFile flowFile = session.get();
        final ComponentLog logger = getLogger();
        if (!isScheduled()) {
            session.rollback();
            return;
        }
        final AtomicReference<String> input = new AtomicReference<>(null);
        try {
            session.read(flowFile, rawIn -> {
                final InputStream in = new BufferedInputStream(rawIn);
                String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                input.set(output);
            });
        } catch (final Exception e) {
            logger.error("Input reading failed {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;

        }
        final String attrMappingStrategy = String.valueOf(context.getProperty(ATTR_MAPPING_STRATEGY));
        String query = null;
        QueryProcessor queryProcessor = null;
        if (attrMappingStrategy.equals(MAP_LIST)) {
            String mappingList = context.getProperty(MAPPING_LIST).getValue();
            List<String> mappedAttributes = List.of(mappingList.split(","));
            query = processQuery(attrMappingStrategy, context.getProperty(XQUERY_SCRIPT).getValue(), mappedAttributes, flowFile);
            queryProcessor = new QueryProcessor(query, basexContext);
            processAttributes(queryProcessor, flowFile, mappedAttributes, logger, session);
        } else if (attrMappingStrategy.equals(MAP_ALL)) {
            query = processQuery(attrMappingStrategy, context.getProperty(XQUERY_SCRIPT).getValue(), flowFile);
            queryProcessor = new QueryProcessor(query, basexContext);
            processAttributes(queryProcessor, flowFile, logger, session);
        }else{
            query = context.getProperty(XQUERY_SCRIPT).getValue();
            queryProcessor = new QueryProcessor(query, basexContext);
        }

        Value result = null;
        try {
            Item node = new DBNode(new IOContent(input.get()));
            queryProcessor.context(node);
            result = queryProcessor.value();
        } catch (Exception e) {
            logger.error("Query failed: ", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        Value finalResult = result;
        String finalResultSerialized = null;
        try {
            finalResultSerialized = finalResult.serialize().toString();
        } catch (Exception e) {
            logger.error("Failed during serializing output: ", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;

        }
        FlowFile flowFileCloneOriginal = session.clone(flowFile);
        if (context.getProperty(OUTPUT_BODY_ATTRIBUTE_NAME).isSet()) {
            String attributeKey = context.getProperty(OUTPUT_BODY_ATTRIBUTE_NAME).getValue();
            flowFileCloneOriginal = session.putAttribute(flowFileCloneOriginal, attributeKey, finalResultSerialized);
        }
        String finalResultSerializedClone = finalResultSerialized;
        flowFile = session.write(flowFile, out -> {
            out.write(finalResultSerializedClone.getBytes(StandardCharsets.UTF_8));

        });
        route(flowFileCloneOriginal, flowFile, session);
    }


    private void processAttributes(QueryProcessor queryProcessor,
                                   FlowFile flowFile,
                                   List<String> mappedAttributes,
                                   ComponentLog logger, ProcessSession session) {
        StringBuilder sb = new StringBuilder();
        flowFile.getAttributes().forEach((key, value) -> {
            try {
                if (mappedAttributes.contains(key)) {
                    sb.append(String.format("declare variable $%s external; ", key));
                    queryProcessor.variable(key, value, null);
                }
            } catch (Exception e) {
                logger.error("Query failed while processing attribute: {}", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        });
    }
    private void processAttributes(QueryProcessor queryProcessor,
                                   FlowFile flowFile,
                                   ComponentLog logger, ProcessSession session) {
        StringBuilder sb = new StringBuilder();
        flowFile.getAttributes().forEach((key, value) -> {
            try {
                sb.append(String.format("declare variable $%s external; ", key));
                queryProcessor.variable(key, value, null);

            } catch (Exception e) {
                logger.error("Query failed while processing attribute: {}", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        });
    }
    private String processQuery(String attributeMappingStrategy, String query, List<String> mappedAttributes, FlowFile flowFile, ComponentLog logger){
        StringBuilder sb = new StringBuilder();
        if (attributeMappingStrategy.equals(MAP_LIST)) {
            flowFile.getAttributes().forEach((key, value) -> {
                if (mappedAttributes.contains(key)) {
                    sb.append(String.format("declare variable $%s external; ", key));
                } else if (attributeMappingStrategy.equals(MAP_ALL)) {
                    sb.append(String.format("declare variable $%s external; ", key));
                }
            });
        }
        return sb.toString() + query;
    }
    private String processQuery(String attributeMappingStrategy, String query, List<String> mappedAttributes, FlowFile flowFile) {
        StringBuilder sb = new StringBuilder();
        flowFile.getAttributes().forEach((key, value) -> {
            if (mappedAttributes.contains(key)) {
                sb.append(String.format("declare variable $%s external; ", key));
            }
        });
        return sb.toString() + query;
    }

    private String processQuery(String attributeMappingStrategy, String query, FlowFile flowFile){
        StringBuilder sb = new StringBuilder();
        flowFile.getAttributes().forEach((key, value) -> {
            sb.append(String.format("declare variable $%s external; ", key));
        });
        return sb.toString() + query;
    }

    private void route(FlowFile flowFileCloneOriginal, FlowFile flowFile, ProcessSession session){
        if (flowFileCloneOriginal != null) {
            session.transfer(flowFileCloneOriginal, REL_ORIGINAL);
        }
        if (flowFile != null) {
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private static class BaseXqueryValidator implements Validator {

        private static final Pattern UNDEFINED_VARIABLE_PATTERN = Pattern.compile("\\bvariable\\b", Pattern.CASE_INSENSITIVE);

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            String errorExplanation = null;

            try {
                Context context = new Context();
                QueryProcessor proc = new QueryProcessor(input, context);

                try {
                    proc.compile();
                } catch (final Exception e) {
                    if (!isUndefinedVariableError(e)) {
                        errorExplanation = e.getMessage();
                    }
                }

            } catch (final Exception e) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation("Failed to initialize or validate XQuery: " + e.getMessage())
                        .build();
            }

            return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(errorExplanation == null)
                    .explanation(errorExplanation)
                    .build();
        }

        private boolean isUndefinedVariableError(Exception e) {
            String message = e.getMessage();
            return message != null && UNDEFINED_VARIABLE_PATTERN.matcher(message).find();
        }
    }

}



