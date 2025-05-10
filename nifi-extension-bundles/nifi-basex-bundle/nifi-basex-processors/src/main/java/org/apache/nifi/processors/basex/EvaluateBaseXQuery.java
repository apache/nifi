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


import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
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
import org.basex.core.Prop;
import org.basex.io.IOContent;
import org.basex.query.QueryException;
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


@SideEffectFree
@SupportsBatching
@Tags({"XML", "evaluate", "XPath", "XQuery", "basex"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "Executes an XQuery expression using the BaseX engine, passing the FlowFile content as a string context to the script. "
                + "An XQuery script must be provided for the processor to run. Users can choose how FlowFile attributes are mapped "
                + "to external variables: map all attributes, map only a specific list, or ignore attributes altogether. "
                + "The output of the query can be written to a specified FlowFile attribute (similarly to the InvokeHTTP processor) "
                + "or used to replace the content of the original FlowFile. The processor defines the following relationships: "
                + "'success' (on successful execution), 'failure' (if the script fails), and 'original' (preserves the unmodified FlowFile)."
)
@SystemResourceConsiderations({
        @SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Processing requires reading the entire FlowFile into memory")
})
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
            .description("FlowFile attribute name used to write an xquery output body for FlowFiles transferred to the Original relationship.")
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
        final List<FlowFile> flowFileBatch = session.get(50);
        final ComponentLog logger = getLogger();
        if (flowFileBatch.isEmpty()) {
            return;
        }
        for (FlowFile transformed : flowFileBatch) {

            if (!isScheduled()) {
                session.rollback();
                return;
            }
            final AtomicReference<String> input = new AtomicReference<>(null);
            try {
                session.read(transformed, rawIn -> {
                    final InputStream in = new BufferedInputStream(rawIn);
                    input.set(new String(in.readAllBytes(), StandardCharsets.UTF_8));
                });
            } catch (final Exception e) {
                logger.error("Input reading failed {}", transformed, e);
                session.transfer(transformed, REL_FAILURE);
                continue;
            }
            Prop prop = new Prop();
            String query = context.getProperty(XQUERY_SCRIPT).getValue();
            final String attrMappingStrategy = String.valueOf(context.getProperty(ATTR_MAPPING_STRATEGY));

            QueryProcessor queryProcessor = new QueryProcessor(query, basexContext);
            if (attrMappingStrategy.equals(MAP_LIST)) {
                String mappingList = context.getProperty(MAPPING_LIST).getValue();
                List<String> mappedAttributes = List.of(mappingList.split(","));
                processAttributes(queryProcessor, transformed, mappedAttributes, logger, session);
            }else if (attrMappingStrategy.equals(MAP_ALL)) {
                processAttributes(queryProcessor, transformed, logger, session);
            }

            Value result = null;
            try {
                Item node = new DBNode(new IOContent(input.get()), prop);
                queryProcessor.context(node);
                result = queryProcessor.value();
            } catch (Exception e) {
                logger.error("Query failed: ", transformed, e);
                session.transfer(transformed, REL_FAILURE);
                continue;
            }

            Value finalResult = result;
            String finalResultSerialized = null;
            try {
                 finalResultSerialized = finalResult.serialize().toString();
            } catch (QueryException e) {
                logger.error("Failed during serializing output: ", transformed, e);
                session.transfer(transformed, REL_FAILURE);
                continue;
            }
            FlowFile original = session.clone(transformed);
            if (context.getProperty(OUTPUT_BODY_ATTRIBUTE_NAME).isSet()) {
                String attributeKey = context.getProperty(OUTPUT_BODY_ATTRIBUTE_NAME).getValue();
                original = session.putAttribute(original, attributeKey, finalResultSerialized);
            }

            String finalResultSerializedClone = finalResultSerialized;

            transformed = session.write(transformed, out -> {
                    out.write(finalResultSerializedClone.getBytes(StandardCharsets.UTF_8));

            });

            route(original, transformed, session);

        }



    }


    private void processAttributes(QueryProcessor queryProcessor,
                                                  FlowFile flowFile,
                                                  List<String> mappedAttributes,
                                                  ComponentLog logger, ProcessSession session)
    {
        flowFile.getAttributes().forEach((key, value) -> {
            try {
                if (mappedAttributes.contains(key)) {
                    queryProcessor.bind(key, value);
                }
            } catch (Exception e) {
                logger.error("Query failed while processing attribute: {}", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        });
    }
    private void processAttributes(QueryProcessor queryProcessor,
                                                     FlowFile flowFile,
                                                     ComponentLog logger, ProcessSession session)
    {
        flowFile.getAttributes().forEach((key, value) -> {
            try {
                queryProcessor.bind(key, value);

            } catch (Exception e) {
                logger.error("Query failed while processing attribute: {}", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        });
    }
    private void route(FlowFile original, FlowFile transformed, ProcessSession session){
        if (original != null) {
            session.transfer(original, REL_ORIGINAL);
        }
        if (transformed != null) {
            session.transfer(transformed, REL_SUCCESS);
        }
    }

    private static class BaseXqueryValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                Context context = new Context();
                final QueryProcessor proc = new QueryProcessor(input, context);
                String error = null;
                try {
                    proc.compile();
                } catch (final Exception e) {
                    error = e.toString().contains("variable") ? null : e.toString();
                }
                return new ValidationResult.Builder().input(input).subject(subject).valid(error == null).explanation(error).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Unable to initialize XQuery engine due to " + e).build();
            }
        }
    }

}



