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
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@EventDriven
@Tags({"mongodb", "insert", "record", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor is a record-aware processor for inserting data into MongoDB. It uses a configured record reader and " +
        "schema to read an incoming record set from the body of a flowfile and then inserts batches of those records into " +
        "a configured MongoDB collection. This processor does not support updates, deletes or upserts. The number of documents to insert at a time is controlled " +
        "by the \"Insert Batch Size\" configuration property. This value should be set to a reasonable size to ensure " +
        "that MongoDB is not overloaded with too many inserts at once.")
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        final FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        final WriteConcern writeConcern = getWriteConcern(context);

        List<Document> inserts = new ArrayList<>();
        int ceiling = context.getProperty(INSERT_COUNT).asInteger();
        int added   = 0;
        boolean error = false;

        try (final InputStream inStream = session.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, inStream, getLogger())) {

            final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);

            Record record;

            while ((record = reader.nextRecord()) != null) {

                // Convert each Record to HashMap and put into the Mongo document

                inserts.add(convertArrays(new Document(buildRecord(record))));

                // If inserts pending reach a specific level, trigger a write
                if (inserts.size() == ceiling) {
                    collection.insertMany(inserts);
                    added += inserts.size();
                    inserts = new ArrayList<>();
                }
            }

            // Flush any pending inserts
            if (!inserts.isEmpty()) {
                collection.insertMany(inserts);
            }
        } catch (SchemaNotFoundException | IOException | MalformedRecordException | MongoException e) {
            getLogger().error("PutMongoRecord failed with error:", e);
            session.transfer(flowFile, REL_FAILURE);
            error = true;
        } catch(Exception ex) {
            getLogger().error("PutMongoRecord failed with error:", ex);
            session.transfer(flowFile, REL_FAILURE);
            error = true;
        } finally {
            if (!error) {
                String url = clientService != null ? clientService.getURI() : context.getProperty(URI).evaluateAttributeExpressions().getValue();
                session.getProvenanceReporter().send(flowFile, url, String.format("Added %d documents to MongoDB.", added));
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Inserted {} records into MongoDB", new Object[]{ added });
            }
        }
        session.commit();
    }

    @SuppressWarnings("unchecked")
    private Document convertArrays(Document doc) {
        Document retVal = new Document();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getClass().isArray()) {
                retVal.put(entry.getKey(), convertArrays((Object[])entry.getValue()));
            } else if (entry.getValue() != null && (entry.getValue() instanceof Map || entry.getValue() instanceof Document)) {
                retVal.put(entry.getKey(), convertArrays(new Document((Map<String,Object>) entry.getValue())));
            } else {
                retVal.put(entry.getKey(), entry.getValue());
            }
        }

        return retVal;
    }

    @SuppressWarnings("unchecked")
    private List<Object> convertArrays(Object[] input) {
        List<Object> retVal = new ArrayList<>();
        for (Object o : input) {
            if (o != null && o.getClass().isArray()) {
                retVal.add(convertArrays((Object[])o));
            } else if (o instanceof Map) {
                retVal.add(convertArrays(new Document((Map<String,Object>)o)));
            } else {
                retVal.add(o);
            }
        }

        return retVal;
    }

    private Map<String,Object> buildRecord(final Record record) {

        Map<String,Object> result = new HashMap<>();

        for(RecordField field : record.getSchema().getFields()) {

            result.put(field.getFieldName(), mapValue(field.getFieldName(), record.getValue(field.getFieldName()), field.getDataType()));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private Object mapValue(final String fieldName, final Object value, final DataType dataType) {

        if(value == null) {

            return null;
        }

        final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType) : dataType;
        final Object coercedValue = DataTypeUtils.convertType(value, chosenDataType, fieldName);

        if(coercedValue == null) {

            return null;
        }

        switch (chosenDataType.getFieldType()) {

            case DATE:
            case TIME:
            case DOUBLE:
            case FLOAT:
            case LONG:
            case INT:
            case BYTE:
            case SHORT:
            case CHAR:
            case STRING:
            case BIGINT:
            case BOOLEAN:
                return coercedValue;

            case TIMESTAMP:
                return ((Timestamp) coercedValue).toInstant();

            case DECIMAL:
                return new Decimal128((BigDecimal) coercedValue);

            case RECORD:
                return buildRecord((Record) coercedValue);

            case ARRAY:
                // Map the value of each element of the array
                return Arrays.stream((Object[]) coercedValue)
                    .map(v -> mapValue(fieldName, v, ((ArrayDataType) chosenDataType).getElementType())).collect(Collectors.toList()).toArray();

            case MAP: {
                // Map the values of each entry in the map

                Map<String,Object> result = new HashMap<>();

                for(Map.Entry<String,Object> entry : ((Map<String, Object>) coercedValue).entrySet()) {

                    result.put(entry.getKey(), mapValue(entry.getKey(), entry.getValue(), ((MapDataType) chosenDataType).getValueType()));
                }

                return result;
            }

            default:
                return coercedValue.toString();
        }
    }
}
