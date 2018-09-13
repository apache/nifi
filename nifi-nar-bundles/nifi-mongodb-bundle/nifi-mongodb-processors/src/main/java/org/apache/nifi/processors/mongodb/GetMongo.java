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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "mongodb", "read", "get" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Creates FlowFiles from documents in MongoDB loaded by a user-specified query.")
@WritesAttributes({
    @WritesAttribute(attribute = GetMongo.DB_NAME, description = "The database where the results came from."),
    @WritesAttribute(attribute = GetMongo.COL_NAME, description = "The collection where the results came from.")
})
public class GetMongo extends AbstractMongoQueryProcessor {


    static final AllowableValue YES_PP = new AllowableValue("true", "True");
    static final AllowableValue NO_PP  = new AllowableValue("false", "False");

    static final PropertyDescriptor USE_PRETTY_PRINTING = new PropertyDescriptor.Builder()
            .name("use-pretty-printing")
            .displayName("Pretty Print Results JSON")
            .description("Choose whether or not to pretty print the JSON from the results of the query. " +
                    "Choosing 'True' can greatly increase the space requirements on disk depending on the complexity of the JSON document")
            .required(true)
            .defaultValue(YES_PP.getValue())
            .allowableValues(YES_PP, NO_PP)
            .addValidator(Validator.VALID)
            .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;
    private ComponentLog logger;

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
        _propertyDescriptors.add(DATE_FORMAT);
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
        logger = getLogger();

        if (context.hasIncomingConnection()) {
            input = session.get();
            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final Document query = getQuery(context, session, input );

        if (query == null) {
            return;
        }

        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        final String usePrettyPrint  = context.getProperty(USE_PRETTY_PRINTING).getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(input).getValue());


        final Document projection = context.getProperty(PROJECTION).isSet()
                ? Document.parse(context.getProperty(PROJECTION).evaluateAttributeExpressions(input).getValue()) : null;
        final Document sort = context.getProperty(SORT).isSet()
                ? Document.parse(context.getProperty(SORT).evaluateAttributeExpressions(input).getValue()) : null;

        final String dateFormat      = context.getProperty(DATE_FORMAT).evaluateAttributeExpressions(input).getValue();
        configureMapper(jsonTypeSetting, dateFormat);

        final MongoCollection<Document> collection = getCollection(context, input);
        final FindIterable<Document> it = collection.find(query);
        final Map<String, String> attributes = getAttributes(context, input, query, collection);

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

        try (MongoCursor<Document> cursor = it.iterator()) {
            configureMapper(jsonTypeSetting, dateFormat);

            if (context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
                int sizePerBatch = context.getProperty(RESULTS_PER_FLOWFILE).evaluateAttributeExpressions(input).asInteger();
                List<Document> batch = new ArrayList<>();

                while (cursor.hasNext()) {
                    batch.add(cursor.next());

                    if (batch.size() == sizePerBatch) {
                        try {
                            writeBatch(buildBatch(batch, jsonTypeSetting, usePrettyPrint), input, context, session, attributes, REL_SUCCESS);
                            batch = new ArrayList<>();
                        } catch (Exception e) {
                            logger.error("Error building batch due to {}", new Object[] {e});
                        }
                    }
                }

                if (batch.size() > 0) {
                    try {
                        writeBatch(buildBatch(batch, jsonTypeSetting, usePrettyPrint), input, context, session, attributes, REL_SUCCESS);
                    } catch (Exception e) {
                        logger.error("Error building batch due to {}", new Object[] {e});
                    }
                }
            } else {
                FlowFile outgoingFlowFile;

                while (cursor.hasNext()) {
                    outgoingFlowFile = (input == null) ? session.create() : session.create(input);
                    outgoingFlowFile = session.write(outgoingFlowFile, out -> {
                        if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                            out.write(getObjectWriter(objectMapper, usePrettyPrint).writeValueAsString(cursor.next()).getBytes(charset));
                        } else {
                            out.write(cursor.next().toJson().getBytes(charset));
                        }
                    });

                    outgoingFlowFile = session.putAllAttributes(outgoingFlowFile, attributes);
                    session.getProvenanceReporter().receive(outgoingFlowFile, getURI(context));
                    session.transfer(outgoingFlowFile, REL_SUCCESS);
                }
            }

            if (input != null) {
                session.transfer(input, REL_ORIGINAL);
            }
        }

    }
}
