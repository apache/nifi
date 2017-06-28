
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

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EventDriven
@Tags({"mongodb", "insert", "record", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Bulk ingest documents into MonogDB using a configured record reader.")
public class PutMongoRecord extends AbstractMongoProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor INSERT_COUNT = new PropertyDescriptor.Builder()
            .name("insert_count")
            .displayName("Insert Batch Size")
            .description("The number of records to group together for one single insert operation against MongoDB.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(WRITE_CONCERN);
        _propertyDescriptors.add(RECORD_READER_FACTORY);
        _propertyDescriptors.add(INSERT_COUNT);
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

        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        final WriteConcern writeConcern = getWriteConcern(context);

        final MongoCollection<Document> collection = getCollection(context).withWriteConcern(writeConcern);

        List<Document> inserts = new ArrayList<>();
        int ceiling = context.getProperty(INSERT_COUNT).asInteger();
        int added   = 0;
        boolean error = false;

        try (RecordReader reader = recordParserFactory.createRecordReader(flowFile, session.read(flowFile), getLogger())) {
            RecordSchema schema = reader.getSchema();
            Record record;
            while ((record = reader.nextRecord()) != null) {
                Document document = new Document();
                for (String name : schema.getFieldNames()) {
                    document.put(name, record.getValue(name));
                }
                inserts.add(document);
                if (inserts.size() == ceiling) {
                    collection.insertMany(inserts);
                    added += inserts.size();
                    inserts = new ArrayList<>();
                }
            }
            if (inserts.size() > 0) {
                collection.insertMany(inserts);
            }
        } catch (SchemaNotFoundException | IOException | MalformedRecordException e) {
            getLogger().error("PutMongoRecord failed with error:", e);
            session.transfer(flowFile, REL_FAILURE);
            error = true;
        } finally {
            if (!error) {
                session.getProvenanceReporter().send(flowFile, context.getProperty(URI).getValue(), String.format("Added %d documents to MongoDB.", added));
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Inserted {} records into MongoDB", new Object[]{ added });
            }
        }
        session.commit();
/*        final ComponentLog logger = getLogger();

        if (inserts.size() > 0) {
            try {
                collection.insertMany(inserts);

                session.getProvenanceReporter().send(flowFile, context.getProperty(URI).getValue());
                session.transfer(flowFile, REL_SUCCESS);

            } catch (Exception e) {
                logger.error("Failed to insert {} into MongoDB due to {}", new Object[]{flowFile, e}, e);
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
            }
        }*/
    }
}
