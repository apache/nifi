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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.bson.Document;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

@EventDriven
@Tags({ "mongodb", "insert", "update", "write", "put" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to MongoDB")
public class PutMongo extends AbstractMongoProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    static final String MODE_INSERT = "insert";
    static final String MODE_UPDATE = "update";

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
                + "otherwise it is ignored")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("_id")
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final String mode = context.getProperty(MODE).getValue();
        final WriteConcern writeConcern = getWriteConcern(context);

        final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);

        try {
            // Read the contents of the FlowFile into a byte array
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });

            // parse
            final Document doc = Document.parse(new String(content, charset));

            if (MODE_INSERT.equalsIgnoreCase(mode)) {
                collection.insertOne(doc);
                logger.info("inserted {} into MongoDB", new Object[] { flowFile });
            } else {
                // update
                final boolean upsert = context.getProperty(UPSERT).asBoolean();
                final String updateKey = context.getProperty(UPDATE_QUERY_KEY).getValue();
                final Document query = new Document(updateKey, doc.get(updateKey));

                collection.replaceOne(query, doc, new UpdateOptions().upsert(upsert));
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
