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

import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
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
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"mongodb", "insert", "update", "upsert", "record", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor is a record-aware processor for inserting/upserting data into MongoDB. It uses a configured record reader and " +
        "schema to read an incoming record set from the body of a flowfile and then inserts/upserts batches of those records into " +
        "a configured MongoDB collection. This processor does not support deletes. The number of documents to insert/upsert at a time is controlled " +
        "by the \"Batch Size\" configuration property. This value should be set to a reasonable size to ensure " +
        "that MongoDB is not overloaded with too many operations at once.")
@ReadsAttribute(
    attribute = PutMongoRecord.MONGODB_UPDATE_MODE,
    description = "Configurable parameter for controlling update mode on a per-flowfile basis." +
        " Acceptable values are 'one' and 'many' and controls whether a single incoming record should update a single or multiple Mongo documents."
)
public class PutMongoRecord extends AbstractMongoProcessor {
    static final String MONGODB_UPDATE_MODE = "mongodb.update.mode";

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    static final AllowableValue UPDATE_ONE = new AllowableValue("one", "Update One", "Updates only the first document that matches the query.");
    static final AllowableValue UPDATE_MANY = new AllowableValue("many", "Update Many", "Updates every document that matches the query.");
    static final AllowableValue UPDATE_FF_ATTRIBUTE = new AllowableValue("flowfile-attribute", "Use '" + MONGODB_UPDATE_MODE + "' flowfile attribute.",
        "Use the value of the '" + MONGODB_UPDATE_MODE + "' attribute of the incoming flowfile. Acceptable values are 'one' and 'many'.");

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor INSERT_COUNT = new PropertyDescriptor.Builder()
            .name("insert_count")
            .displayName("Batch Size")
            .description("The number of records to group together for one single insert/upsert operation against MongoDB.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor ORDERED = new PropertyDescriptor.Builder()
            .name("ordered")
            .displayName("Ordered")
            .description("Perform ordered or unordered operations")
            .allowableValues("True", "False")
            .defaultValue("False")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor BYPASS_VALIDATION = new PropertyDescriptor.Builder()
            .name("bypass-validation")
            .displayName("Bypass Validation")
            .description("""
                    Enable or disable bypassing document schema validation during insert or update operations.
                    Bypassing document validation is a Privilege Action in MongoDB.
                    Enabling this property can result in authorization errors for users with limited privileges.
            """)
            .allowableValues("True", "False")
            .defaultValue("False")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor UPDATE_KEY_FIELDS = new PropertyDescriptor.Builder()
            .name("update-key-fields")
            .displayName("Update Key Fields")
            .description("Comma separated list of fields based on which to identify documents that need to be updated. " +
                "If this property is set NiFi will attempt an upsert operation on all documents. " +
                "If this property is not set all documents will be inserted.")
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, false, StandardValidators.NON_EMPTY_VALIDATOR))
            .build();

    static final PropertyDescriptor UPDATE_MODE = new PropertyDescriptor.Builder()
        .name("update-mode")
        .displayName("Update Mode")
        .dependsOn(UPDATE_KEY_FIELDS)
        .description("Choose between updating a single document or multiple documents per incoming record.")
        .allowableValues(UPDATE_ONE, UPDATE_MANY, UPDATE_FF_ATTRIBUTE)
        .defaultValue(UPDATE_ONE.getValue())
        .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(RECORD_READER_FACTORY);
        _propertyDescriptors.add(INSERT_COUNT);
        _propertyDescriptors.add(ORDERED);
        _propertyDescriptors.add(BYPASS_VALIDATION);
        _propertyDescriptors.add(UPDATE_KEY_FIELDS);
        _propertyDescriptors.add(UPDATE_MODE);
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

        final WriteConcern writeConcern = clientService.getWriteConcern();

        int ceiling = context.getProperty(INSERT_COUNT).asInteger();
        int written = 0;
        boolean error = false;

        boolean ordered = context.getProperty(ORDERED).asBoolean();
        boolean bypass = context.getProperty(BYPASS_VALIDATION).asBoolean();

        Map<String, List<String>> updateKeyFieldPathToFieldChain = new LinkedHashMap<>();
        if (context.getProperty(UPDATE_KEY_FIELDS).isSet()) {
            Arrays.stream(context.getProperty(UPDATE_KEY_FIELDS).getValue().split("\\s*,\\s*"))
                .forEach(updateKeyField -> updateKeyFieldPathToFieldChain.put(
                    updateKeyField,
                    Arrays.asList(updateKeyField.split("\\."))
                ));
        }

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
        bulkWriteOptions.ordered(ordered);
        bulkWriteOptions.bypassDocumentValidation(bypass);

        try (
            final InputStream inStream = session.read(flowFile);
            final RecordReader reader = recordParserFactory.createRecordReader(flowFile, inStream, getLogger());
        ) {
            RecordSchema schema = reader.getSchema();

            final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);

            List<WriteModel<Document>> writeModels = new ArrayList<>();

            Record record;
            while ((record = reader.nextRecord()) != null) {
                // Convert each Record to HashMap and put into the Mongo document
                Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
                Document document = new Document();
                for (String name : schema.getFieldNames()) {
                    document.put(name, contentMap.get(name));
                }
                Document readyToUpsert = convertArrays(document);

                WriteModel<Document> writeModel;
                if (context.getProperty(UPDATE_KEY_FIELDS).isSet()) {
                    Bson[] filters = buildFilters(updateKeyFieldPathToFieldChain, readyToUpsert);

                    if (updateModeMatches(UPDATE_ONE.getValue(), context, flowFile)) {
                        writeModel = new UpdateOneModel<>(
                            Filters.and(filters),
                            new Document("$set", readyToUpsert),
                            new UpdateOptions().upsert(true)
                        );
                    } else if (updateModeMatches(UPDATE_MANY.getValue(), context, flowFile)) {
                        writeModel = new UpdateManyModel<>(
                            Filters.and(filters),
                            new Document("$set", readyToUpsert),
                            new UpdateOptions().upsert(true)
                        );
                    } else {
                        String flowfileUpdateMode = flowFile.getAttribute(MONGODB_UPDATE_MODE);
                        throw new ProcessException("Unrecognized '" + MONGODB_UPDATE_MODE + "' value '" + flowfileUpdateMode + "'");
                    }
                } else {
                    writeModel = new InsertOneModel<>(readyToUpsert);
                }

                writeModels.add(writeModel);
                if (writeModels.size() == ceiling) {
                    collection.bulkWrite(writeModels, bulkWriteOptions);
                    written += writeModels.size();
                    writeModels = new ArrayList<>();
                }
            }
            if (writeModels.size() > 0) {
                collection.bulkWrite(writeModels, bulkWriteOptions);
            }
        } catch (ProcessException | SchemaNotFoundException | IOException | MalformedRecordException | MongoException e) {
            getLogger().error("PutMongoRecord failed with error:", e);
            session.transfer(flowFile, REL_FAILURE);
            error = true;
        } finally {
            if (!error) {
                session.getProvenanceReporter().send(flowFile, clientService.getURI(), String.format("Written %d documents to MongoDB.", written));
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Written {} records into MongoDB", new Object[]{ written });
            }
        }
    }

    private Document convertArrays(Document doc) {
        Document retVal = new Document();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getClass().isArray()) {
                retVal.put(entry.getKey(), convertArrays((Object[])entry.getValue()));
            } else if (entry.getValue() != null && (entry.getValue() instanceof Map || entry.getValue() instanceof Document)) {
                retVal.put(entry.getKey(), convertArrays(new Document((Map)entry.getValue())));
            } else {
                retVal.put(entry.getKey(), entry.getValue());
            }
        }

        return retVal;
    }

    private List convertArrays(Object[] input) {
        List retVal = new ArrayList();
        for (Object o : input) {
            if (o != null && o.getClass().isArray()) {
                retVal.add(convertArrays((Object[])o));
            } else if (o instanceof Map) {
                retVal.add(convertArrays(new Document((Map)o)));
            } else {
                retVal.add(o);
            }
        }

        return retVal;
    }

    private Bson[] buildFilters(Map<String, List<String>> updateKeyFieldPathToFieldChain, Document readyToUpsert) {
        Bson[] filters = updateKeyFieldPathToFieldChain.entrySet()
            .stream()
            .map(updateKeyFieldPath__fieldChain -> {
                String fieldPath = updateKeyFieldPath__fieldChain.getKey();
                List<String> fieldChain = updateKeyFieldPath__fieldChain.getValue();

                Object value = readyToUpsert;
                String previousField = null;
                for (String field : fieldChain) {
                    if (!(value instanceof Map)) {
                        throw new ProcessException("field '" + previousField + "' (from field expression '" + fieldPath + "') is not an embedded document");
                    }

                    value = ((Map) value).get(field);

                    if (value == null) {
                        throw new ProcessException("field '" + field + "' (from field expression '" + fieldPath + "') has no value");
                    }

                    previousField = field;
                }

                Bson filter = Filters.eq(fieldPath, value);
                return filter;
            })
            .toArray(Bson[]::new);

        return filters;
    }

    private boolean updateModeMatches(String updateModeToMatch, ProcessContext context, FlowFile flowFile) {
        String updateMode = context.getProperty(UPDATE_MODE).getValue();

        boolean updateModeMatches = updateMode.equals(updateModeToMatch)
            ||
            (
                updateMode.equals(UPDATE_FF_ATTRIBUTE.getValue())
                    && updateModeToMatch.equalsIgnoreCase(flowFile.getAttribute(MONGODB_UPDATE_MODE))
            );

        return updateModeMatches;
    }
}
