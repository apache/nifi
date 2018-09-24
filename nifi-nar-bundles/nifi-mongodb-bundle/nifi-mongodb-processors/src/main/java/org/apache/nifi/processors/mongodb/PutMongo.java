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
package org.apache.nifi.processors.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.util.JSON;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@EventDriven
@Tags({ "mongodb", "insert", "update", "write", "put" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to MongoDB")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutMongo extends AbstractMongoProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    static final String MODE_INSERT = "insert";
    static final String MODE_UPDATE = "update";

    static final AllowableValue UPDATE_WITH_DOC = new AllowableValue("doc", "With whole document");
    static final AllowableValue UPDATE_WITH_OPERATORS = new AllowableValue("operators", "With operators enabled");

    static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
        .name("Mode")
        .description("Indicates whether the processor should insert or update content")
        .required(true)
        .allowableValues(MODE_INSERT, MODE_UPDATE)
        .defaultValue(MODE_INSERT)
        .build();
    static final PropertyDescriptor UPSERT = new PropertyDescriptor.Builder()
        .name("Upsert")
        .description("When true, inserts a document if no document matches the update query criteria; this property is valid only when using update mode, "
                + "otherwise it is ignored")
        .required(true)
        .allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build();
    static final PropertyDescriptor UPDATE_QUERY_KEY = new PropertyDescriptor.Builder()
        .name("Update Query Key")
        .description("Key name used to build the update query criteria; this property is valid only when using update mode, "
                + "otherwise it is ignored. Example: _id")
        .required(false)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor UPDATE_QUERY = new PropertyDescriptor.Builder()
        .name("putmongo-update-query")
        .displayName("Update Query")
        .description("Specify a full MongoDB query to be used for the lookup query to do an update/upsert.")
        .required(false)
        .addValidator(JsonValidator.INSTANCE)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor UPDATE_MODE = new PropertyDescriptor.Builder()
        .displayName("Update Mode")
        .name("put-mongo-update-mode")
        .required(true)
        .allowableValues(UPDATE_WITH_DOC, UPDATE_WITH_OPERATORS)
        .defaultValue(UPDATE_WITH_DOC.getValue())
        .description("Choose an update mode. You can either supply a JSON document to use as a direct replacement " +
                "or specify a document that contains update operators like $set, $unset, and $inc. " +
                "When Operators mode is enabled, the flowfile content is expected to be the operator part " +
                "for example: {$set:{\"key\": \"value\"},$inc:{\"count\":1234}} and the update query will come " +
                "from the configured Update Query property.")
         .build();
    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character Set in which the data is encoded")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(MODE);
        _propertyDescriptors.add(UPSERT);
        _propertyDescriptors.add(UPDATE_QUERY_KEY);
        _propertyDescriptors.add(UPDATE_QUERY);
        _propertyDescriptors.add(UPDATE_MODE);
        _propertyDescriptors.add(WRITE_CONCERN);
        _propertyDescriptors.add(CHARACTER_SET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        List<ValidationResult> problems = new ArrayList<>();

        if (validationContext.getProperty(MODE).getValue().equals(MODE_INSERT)) {
            return problems;
        }

        final boolean queryKey = validationContext.getProperty(UPDATE_QUERY_KEY).isSet();
        final boolean query    = validationContext.getProperty(UPDATE_QUERY).isSet();

        if (queryKey && query) {
            problems.add(new ValidationResult.Builder()
                .valid(false)
                .explanation("Both update query key and update query cannot be set at the same time.")
                .build()
            );
        } else if (!queryKey && !query) {
            problems.add(new ValidationResult.Builder()
                .valid(false)
                .explanation("Either the update query key or the update query field must be set.")
                .build()
            );
        }

        return problems;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final String mode = context.getProperty(MODE).getValue();
        final String updateMode = context.getProperty(UPDATE_MODE).getValue();
        final WriteConcern writeConcern = getWriteConcern(context);

        try {
            final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);
            // Read the contents of the FlowFile into a byte array
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, content, true));

            // parse
            final Object doc = (mode.equals(MODE_INSERT) || (mode.equals(MODE_UPDATE) && updateMode.equals(UPDATE_WITH_DOC.getValue())))
                    ? Document.parse(new String(content, charset)) : JSON.parse(new String(content, charset));

            if (MODE_INSERT.equalsIgnoreCase(mode)) {
                collection.insertOne((Document)doc);
                logger.info("inserted {} into MongoDB", new Object[] { flowFile });
            } else {
                // update
                final boolean upsert = context.getProperty(UPSERT).asBoolean();
                final String updateKey = context.getProperty(UPDATE_QUERY_KEY).evaluateAttributeExpressions(flowFile).getValue();
                final String filterQuery = context.getProperty(UPDATE_QUERY).evaluateAttributeExpressions(flowFile).getValue();
                final Document query;

                if (!StringUtils.isBlank(updateKey)) {
                    query = parseUpdateKey(updateKey, (Map)doc);
                    removeUpdateKeys(updateKey, (Map)doc);
                } else {
                    query = Document.parse(filterQuery);
                }

                if (updateMode.equals(UPDATE_WITH_DOC.getValue())) {
                    collection.replaceOne(query, (Document)doc, new UpdateOptions().upsert(upsert));
                } else {
                    BasicDBObject update = (BasicDBObject)doc;
                    update.remove(updateKey);
                    collection.updateOne(query, update, new UpdateOptions().upsert(upsert));
                }
                logger.info("updated {} into MongoDB", new Object[] { flowFile });
            }

            session.getProvenanceReporter().send(flowFile, getURI(context));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Failed to insert {} into MongoDB due to {}", new Object[] {flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private void removeUpdateKeys(String updateKeyParam, Map doc) {
        String[] parts = updateKeyParam.split(",[\\s]*");
        for (String part : parts) {
            if (part.contains(".")) {
                doc.remove(part);
            }
        }
    }

    private Document parseUpdateKey(String updateKey, Map doc) {
        Document retVal;
        if (updateKey.equals("_id") && ObjectId.isValid(((String) doc.get(updateKey)))) {
            retVal = new Document("_id", new ObjectId((String) doc.get(updateKey)));
        } else if (updateKey.contains(",")) {
            String[] parts = updateKey.split(",[\\s]*");
            retVal = new Document();
            for (String part : parts) {
                retVal.append(part, doc.get(part));
            }
        } else {
            retVal = new Document(updateKey, doc.get(updateKey));
        }

        return retVal;
    }


    protected WriteConcern getWriteConcern(final ProcessContext context) {
        final String writeConcernProperty = context.getProperty(WRITE_CONCERN).getValue();
        WriteConcern writeConcern = null;
        switch (writeConcernProperty) {
        case WRITE_CONCERN_ACKNOWLEDGED:
            writeConcern = WriteConcern.ACKNOWLEDGED;
            break;
        case WRITE_CONCERN_UNACKNOWLEDGED:
            writeConcern = WriteConcern.UNACKNOWLEDGED;
            break;
        case WRITE_CONCERN_FSYNCED:
            writeConcern = WriteConcern.FSYNCED;
            break;
        case WRITE_CONCERN_JOURNALED:
            writeConcern = WriteConcern.JOURNALED;
            break;
        case WRITE_CONCERN_REPLICA_ACKNOWLEDGED:
            writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
            break;
        case WRITE_CONCERN_MAJORITY:
            writeConcern = WriteConcern.MAJORITY;
            break;
        default:
            writeConcern = WriteConcern.ACKNOWLEDGED;
        }
        return writeConcern;
    }
}
