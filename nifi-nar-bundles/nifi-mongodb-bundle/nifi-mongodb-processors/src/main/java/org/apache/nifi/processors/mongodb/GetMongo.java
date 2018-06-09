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
package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "mongodb", "read", "get" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Creates FlowFiles from documents in MongoDB")
public class GetMongo extends AbstractMongoProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All files are routed to success").build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All input flowfiles that are part of a failed query execution go here.")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("All input flowfiles that are part of a successful query execution go here.")
            .build();

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("Query")
        .description("The selection criteria to do the lookup. If the field is left blank, it will look for input from" +
                " an incoming connection from another processor to provide the query as a valid JSON document inside of " +
                "the flowfile's body. If this field is left blank and a timer is enabled instead of an incoming connection, " +
                "that will result in a full collection fetch using a \"{}\" query.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(JsonValidator.INSTANCE)
        .build();

    static final PropertyDescriptor PROJECTION = new PropertyDescriptor.Builder()
            .name("Projection")
            .description("The fields to be returned from the documents in the result set; must be a valid BSON document")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();
    static final PropertyDescriptor SORT = new PropertyDescriptor.Builder()
            .name("Sort")
            .description("The fields by which to sort; must be a valid BSON document")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();
    static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
            .name("Limit")
            .description("The maximum number of elements to return")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The number of elements returned from the server in one batch")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor RESULTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("results-per-flowfile")
            .displayName("Results Per FlowFile")
            .description("How many results to put into a flowfile at once. The whole body will be treated as a JSON array of results.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final AllowableValue YES_PP = new AllowableValue("true", "True");
    static final AllowableValue NO_PP  = new AllowableValue("false", "False");
    static final PropertyDescriptor USE_PRETTY_PRINTING = new PropertyDescriptor.Builder()
            .name("use-pretty-printing")
            .displayName("Pretty Print Results JSON")
            .description("Choose whether or not to pretty print the JSON from the results of the query. " +
                    "Choosing yes can greatly increase the space requirements on disk depending on the complexity of the JSON document")
            .required(true)
            .defaultValue(YES_PP.getValue())
            .allowableValues(YES_PP, NO_PP)
            .addValidator(Validator.VALID)
            .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(JSON_TYPE);
        _propertyDescriptors.add(USE_PRETTY_PRINTING);
        _propertyDescriptors.add(CHARSET);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(QUERY_ATTRIBUTE);
        _propertyDescriptors.add(PROJECTION);
        _propertyDescriptors.add(SORT);
        _propertyDescriptors.add(LIMIT);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(RESULTS_PER_FLOWFILE);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_ORIGINAL);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    //Turn a list of Mongo result documents into a String representation of a JSON array
    private String buildBatch(List<Document> documents, String jsonTypeSetting, String prettyPrintSetting) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < documents.size(); index++) {
            Document document = documents.get(index);
            String asJson;
            if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                asJson = getObjectWriter(objectMapper, prettyPrintSetting).writeValueAsString(document);
            } else {
                asJson = document.toJson(new JsonWriterSettings(true));
            }
            builder
                    .append(asJson)
                    .append( (documents.size() > 1 && index + 1 < documents.size()) ? ", " : "" );
        }

        return "[" + builder.toString() + "]";
    }

    private ObjectWriter getObjectWriter(ObjectMapper mapper, String ppSetting) {
        return ppSetting.equals(YES_PP.getValue()) ? mapper.writerWithDefaultPrettyPrinter()
                : mapper.writer();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = null;
        if (context.hasIncomingConnection()) {
            input = session.get();

            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ComponentLog logger = getLogger();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

        final Document query;
        String queryStr;
        if (context.getProperty(QUERY).isSet()) {
            queryStr = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
            query = Document.parse(queryStr);
        } else if (!context.getProperty(QUERY).isSet() && input == null) {
            queryStr = "{}";
            query = Document.parse("{}");
        } else {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                session.exportTo(input, out);
                out.close();
                queryStr = new String(out.toByteArray());
                query = Document.parse(queryStr);
            } catch (Exception ex) {
                getLogger().error("Error reading flowfile", ex);
                if (input != null) { //Likely culprit is a bad query
                    session.transfer(input, REL_FAILURE);
                    return;
                } else {
                    throw new ProcessException(ex);
                }
            }
        }

        if (context.getProperty(QUERY_ATTRIBUTE).isSet()) {
            final String queryAttr = context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(input).getValue();
            attributes.put(queryAttr, queryStr);
        }

        final Document projection = context.getProperty(PROJECTION).isSet()
                ? Document.parse(context.getProperty(PROJECTION).evaluateAttributeExpressions(input).getValue()) : null;
        final Document sort = context.getProperty(SORT).isSet()
                ? Document.parse(context.getProperty(SORT).evaluateAttributeExpressions(input).getValue()) : null;
        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        final String usePrettyPrint  = context.getProperty(USE_PRETTY_PRINTING).getValue();
        configureMapper(jsonTypeSetting);


        final MongoCollection<Document> collection = getCollection(context, input);

        try {
            final FindIterable<Document> it = query != null ? collection.find(query) : collection.find();
            if (projection != null) {
                it.projection(projection);
            }
            if (sort != null) {
                it.sort(sort);
            }
            if (context.getProperty(LIMIT).isSet()) {
                it.limit(context.getProperty(LIMIT).evaluateAttributeExpressions(input).asInteger());
            }
            if (context.getProperty(BATCH_SIZE).isSet()) {
                it.batchSize(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions(input).asInteger());
            }

            final MongoCursor<Document> cursor = it.iterator();
            ComponentLog log = getLogger();
            try {
                FlowFile flowFile = null;
                if (context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
                    int ceiling = context.getProperty(RESULTS_PER_FLOWFILE).evaluateAttributeExpressions(input).asInteger();
                    List<Document> batch = new ArrayList<>();

                    while (cursor.hasNext()) {
                        batch.add(cursor.next());
                        if (batch.size() == ceiling) {
                            try {
                                if (log.isDebugEnabled()) {
                                    log.debug("Writing batch...");
                                }
                                String payload = buildBatch(batch, jsonTypeSetting, usePrettyPrint);
                                writeBatch(payload, null, context, session, attributes, REL_SUCCESS);
                                batch = new ArrayList<>();
                            } catch (Exception ex) {
                                getLogger().error("Error building batch", ex);
                            }
                        }
                    }
                    if (batch.size() > 0) {
                        try {
                            writeBatch(buildBatch(batch, jsonTypeSetting, usePrettyPrint), null, context, session, attributes, REL_SUCCESS);
                        } catch (Exception ex) {
                            getLogger().error("Error sending remainder of batch", ex);
                        }
                    }
                } else {
                    while (cursor.hasNext()) {
                        final FlowFile ffPtr = input;
                        flowFile = session.create();
                        flowFile = session.write(flowFile, out -> {
                            String json;
                            if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                                json = getObjectWriter(objectMapper, usePrettyPrint).writeValueAsString(cursor.next());
                            } else {
                                json = cursor.next().toJson();
                            }
                            out.write(json.getBytes(context.getProperty(CHARSET).evaluateAttributeExpressions(ffPtr).getValue()));
                        });
                        flowFile = session.putAllAttributes(flowFile, attributes);

                        session.getProvenanceReporter().receive(flowFile, getURI(context));
                        session.transfer(flowFile, REL_SUCCESS);
                    }
                }

                if (input != null) {
                    session.transfer(input, REL_ORIGINAL);
                }

            } finally {
                cursor.close();
            }

        } catch (final RuntimeException e) {
            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }
            context.yield();
            logger.error("Failed to execute query {} due to {}", new Object[] { query, e }, e);
        }
    }
}
