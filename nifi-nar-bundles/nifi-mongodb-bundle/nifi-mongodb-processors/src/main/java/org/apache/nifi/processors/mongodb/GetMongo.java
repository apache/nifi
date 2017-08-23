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

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@Tags({ "mongodb", "read", "get" })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Creates FlowFiles from documents in MongoDB")
public class GetMongo extends AbstractMongoProcessor {
    public static final Validator DOCUMENT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject(subject).input(value);

            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return builder.valid(true).explanation("Contains Expression Language").build();
            }

            String reason = null;
            try {
                Document.parse(value);
            } catch (final RuntimeException e) {
                reason = e.getLocalizedMessage();
            }

            return builder.explanation(reason).valid(reason == null).build();
        }
    };

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All files are routed to success").build();

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("Query")
        .description("The selection criteria; must be a valid MongoDB Extended JSON format; if omitted the entire collection will be queried")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(DOCUMENT_VALIDATOR)
        .build();
    static final PropertyDescriptor PROJECTION = new PropertyDescriptor.Builder()
        .name("Projection")
        .description("The fields to be returned from the documents in the result set; must be a valid BSON document")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(DOCUMENT_VALIDATOR)
        .build();
    static final PropertyDescriptor SORT = new PropertyDescriptor.Builder()
        .name("Sort")
        .description("The fields by which to sort; must be a valid BSON document")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(DOCUMENT_VALIDATOR)
        .build();
    static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
        .name("Limit")
        .description("The maximum number of elements to return")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The number of elements returned from the server in one batch")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    static final PropertyDescriptor RESULTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
        .name("results-per-flowfile")
        .displayName("Results Per FlowFile")
        .description("How many results to put into a flowfile at once. The whole body will be treated as a JSON array of results.")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final String JSON_TYPE_EXTENDED = "Extended";
    static final String JSON_TYPE_STANDARD   = "Standard";
    static final AllowableValue JSON_EXTENDED = new AllowableValue(JSON_TYPE_EXTENDED, "Extended JSON",
            "Use MongoDB's \"extended JSON\". This is the JSON generated with toJson() on a MongoDB Document from the Java driver");
    static final AllowableValue JSON_STANDARD = new AllowableValue(JSON_TYPE_STANDARD, "Standard JSON",
            "Generate a JSON document that conforms to typical JSON conventions instead of Mongo-specific conventions.");
    static final PropertyDescriptor JSON_TYPE = new PropertyDescriptor.Builder()
            .allowableValues(JSON_EXTENDED, JSON_STANDARD)
            .defaultValue(JSON_TYPE_EXTENDED)
            .displayName("JSON Type")
            .name("json-type")
            .description("By default, MongoDB's Java driver returns \"extended JSON\". Some of the features of this variant of JSON" +
            " may cause problems for other JSON parsers that expect only standard JSON types and conventions. This configuration setting " +
            " controls whether to use extended JSON or provide a clean view that conforms to standard JSON.")
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(JSON_TYPE);
        _propertyDescriptors.add(QUERY);
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

    private ObjectMapper mapper;

    //Turn a list of Mongo result documents into a String representation of a JSON array
    private String buildBatch(List<Document> documents, String jsonTypeSetting) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < documents.size(); index++) {
            Document document = documents.get(index);
            String asJson;
            if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                asJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(document);
            } else {
                asJson = document.toJson(new JsonWriterSettings(true));
            }
            builder
                .append(asJson)
                .append( (documents.size() > 1 && index + 1 < documents.size()) ? ", " : "" );
        }

        return "[" + builder.toString() + "]";
    }

    private void configureMapper(String setting) {
        mapper = new ObjectMapper();
        if (setting.equals(JSON_TYPE_STANDARD)) {
            mapper.registerModule(ObjectIdSerializer.getModule());
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            mapper.setDateFormat(df);
        }
    }

    private void writeBatch(String payload, ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(payload.getBytes("UTF-8"));
            }
        });
        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.getProvenanceReporter().receive(flowFile, getURI(context));
        session.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        final Document query = context.getProperty(QUERY).isSet()
                ? Document.parse(context.getProperty(QUERY).evaluateAttributeExpressions().getValue()) : null;
        final Document projection = context.getProperty(PROJECTION).isSet()
                ? Document.parse(context.getProperty(PROJECTION).evaluateAttributeExpressions().getValue()) : null;
        final Document sort = context.getProperty(SORT).isSet()
                ? Document.parse(context.getProperty(SORT).evaluateAttributeExpressions().getValue()) : null;
        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        configureMapper(jsonTypeSetting);


        final MongoCollection<Document> collection = getCollection(context);

        try {
            final FindIterable<Document> it = query != null ? collection.find(query) : collection.find();
            if (projection != null) {
                it.projection(projection);
            }
            if (sort != null) {
                it.sort(sort);
            }
            if (context.getProperty(LIMIT).isSet()) {
                it.limit(context.getProperty(LIMIT).asInteger());
            }
            if (context.getProperty(BATCH_SIZE).isSet()) {
                it.batchSize(context.getProperty(BATCH_SIZE).asInteger());
            }

            final MongoCursor<Document> cursor = it.iterator();
            ComponentLog log = getLogger();
            try {
                FlowFile flowFile = null;
                if (context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
                    int ceiling = context.getProperty(RESULTS_PER_FLOWFILE).asInteger();
                    List<Document> batch = new ArrayList<>();

                    while (cursor.hasNext()) {
                        batch.add(cursor.next());
                        if (batch.size() == ceiling) {
                            try {
                                if (log.isDebugEnabled()) {
                                    log.debug("Writing batch...");
                                }
                                String payload = buildBatch(batch, jsonTypeSetting);
                                writeBatch(payload, context, session);
                                batch = new ArrayList<>();
                            } catch (IOException ex) {
                                getLogger().error("Error building batch", ex);
                            }
                        }
                    }
                    if (batch.size() > 0) {
                        try {
                            writeBatch(buildBatch(batch, jsonTypeSetting), context, session);
                        } catch (IOException ex) {
                            getLogger().error("Error sending remainder of batch", ex);
                        }
                    }
                } else {
                    while (cursor.hasNext()) {
                        flowFile = session.create();
                        flowFile = session.write(flowFile, new OutputStreamCallback() {
                            @Override
                            public void process(OutputStream out) throws IOException {
                                String json;
                                if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                                    json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cursor.next());
                                } else {
                                    json = cursor.next().toJson();
                                }
                                IOUtils.write(json, out);
                            }
                        });
                        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");

                        session.getProvenanceReporter().receive(flowFile, getURI(context));
                        session.transfer(flowFile, REL_SUCCESS);
                    }
                }

                session.commit();

            } finally {
                cursor.close();
            }

        } catch (final RuntimeException e) {
            context.yield();
            session.rollback();
            logger.error("Failed to execute query {} due to {}", new Object[] { query, e }, e);
        }
    }
}
