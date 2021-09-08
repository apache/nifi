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
package org.apache.nifi.processors.azure.cosmos.document;


import com.azure.cosmos.CosmosException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
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
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@EventDriven
@Tags({ "azure", "cosmos", "insert", "record", "put" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor is a record-aware processor for inserting data into Cosmos DB with Core SQL API. It uses a configured record reader and " +
        "schema to read an incoming record set from the body of a Flowfile and then inserts those records into " +
        "a configured Cosmos DB Container.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@DefaultSettings(yieldDuration = "30 sec", penaltyDuration = "1 min")
public class PutAzureCosmosDBRecord extends AbstractAzureCosmosDBProcessor {

    private String conflictHandlingStrategy;
    static final AllowableValue IGNORE_CONFLICT = AzureCosmosDBUtils.IGNORE_CONFLICT;
    static final AllowableValue UPSERT_CONFLICT = AzureCosmosDBUtils.UPSERT_CONFLICT;

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor INSERT_BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("insert-batch-size")
        .displayName("Insert Batch Size")
        .description("The number of records to group together for one single insert operation against Cosmos DB")
        .defaultValue("20")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final PropertyDescriptor CONFLICT_HANDLE_STRATEGY = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-conflict-handling-strategy")
        .displayName("Cosmos DB Conflict Handling Strategy")
        .description("Choose whether to ignore or upsert when conflict error occurs during insertion")
        .required(false)
        .allowableValues(IGNORE_CONFLICT, UPSERT_CONFLICT)
        .defaultValue(IGNORE_CONFLICT.getValue())
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();


    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(RECORD_READER_FACTORY);
        _propertyDescriptors.add(INSERT_BATCH_SIZE);
        _propertyDescriptors.add(CONFLICT_HANDLE_STRATEGY);
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

    protected void bulkInsert(final List<Map<String, Object>> records, final String partitionKeyField) throws CosmosException, ProcessException {

        Comparator<Map<String,Object>> sortByPartitonKeyField = new Comparator<Map<String, Object>>() {
            public int compare(final Map<String, Object> o1, final Map<String, Object> o2) {
                return (o1.get(partitionKeyField).toString()).compareTo(o2.get(partitionKeyField).toString());
            }
        };
        records.sort(sortByPartitonKeyField);
        String lastPartitionKeyValue = "";
        List<Map<String, Object>> bin = new ArrayList<>();
        for (Map<String, Object> record : records) {
            final String recordPartitionKeyValue = (String)record.get(partitionKeyField);
            if (!lastPartitionKeyValue.equals(recordPartitionKeyValue)) {
                if (bin.size() > 0) {
                    chooseInsertMethodAndRun(bin, partitionKeyField, conflictHandlingStrategy);
                    bin = new ArrayList<>();
                }
            }
            bin.add(record);
            lastPartitionKeyValue = recordPartitionKeyValue;
        }
        if (bin.size() > 0) {
            // insert any leftover at last
            chooseInsertMethodAndRun(bin, partitionKeyField, conflictHandlingStrategy);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        final String partitionKeyField = context.getProperty(PARTITION_KEY).getValue();
        List<Map<String, Object>> batch = new ArrayList<>();
        int ceiling = context.getProperty(INSERT_BATCH_SIZE).asInteger();
        boolean error = false, yield = false;
        try (final InputStream inStream = session.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, inStream, getLogger())) {

            RecordSchema schema = reader.getSchema();
            Record record;

            while ((record = reader.nextRecord()) != null) {
                // Convert each Record to HashMap
                Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(schema));
                if (contentMap.containsKey("id")) {
                    final Object idObj = contentMap.get("id");
                    final String idStr = (idObj == null) ? "" : String.valueOf(idObj);
                    if (idObj == null || StringUtils.isBlank(idStr)) {
                        // dont put null to id
                        contentMap.put("id", UUID.randomUUID().toString());
                    } else {
                        contentMap.put("id", idStr);
                    }
                } else {
                    contentMap.put("id", UUID.randomUUID().toString());
                }
                if (!contentMap.containsKey(partitionKeyField)) {
                    logger.error("PutAzureCosmoDBRecord failed with missing partitionKeyField {}", partitionKeyField);
                    error = true;
                    break;
                }
                batch.add(contentMap);
                if (batch.size() == ceiling) {
                    bulkInsert(batch, partitionKeyField);
                    batch = new ArrayList<>();
                }
            }
            if (!error && batch.size() > 0) {
                bulkInsert(batch, partitionKeyField);
            }
        } catch (final CosmosException e)  {
            final int statusCode =  e.getStatusCode();
            if (statusCode == 429) {
                logger.error("Failure due to server-side throttling. Increase RU setting for your workload");
                yield = true; // to send the current flowfile back to queue and retry later
            } else if (statusCode == 449) {
                logger.error("The operation encountered a transient error. It is safe to retry the operation");
                yield = true; // to send the current flowfile back to queue and retry later;
            }  else if (statusCode == 410) {
                logger.error("A request to change the throughput is currently in progress");
                yield = true; // to send the current flowfile back to queue and retry later
            } else if (statusCode == 413) {
                logger.error("Entity is too large: The max allowable document size is 2 MB");
                error = true; // to transfer the current flowfile to failure
                yield = false;
            } else {
                logger.error("statusCode: {},  subStatusCode: {}", statusCode, e.getSubStatusCode());
                error = true; // to transfer the current flowfile to failure
                yield = false;
            }
        } catch (final ProcessException pe) {
            final String expMsg = pe.getMessage();
            if (expMsg.startsWith("429:")) {
                logger.error("Failure due to server-side throttling. Increase RU setting for your workload");
                yield = true; // to send the current flowfile back to queue and retry later
            } else if (expMsg.startsWith("449:")) {
                logger.error("The operation encountered a transient error. It is safe to retry the operation");
                yield = true; // to send the current flowfile back to queue and retry later;
            }  else if (expMsg.startsWith("410:")) {
                logger.error("A request to change the throughput is currently in progress");
                yield = true; // to send the current flowfile back to queue and retry later
            } else if (expMsg.startsWith("413:")) {
                logger.error("Entity is too large: The max allowable document size is 2 MB");
                error = true; // to transfer the current flowfile to failure
                yield = false;
            } else {
                logger.error(expMsg);
                error = true; // to transfer the current flowfile to failure
                yield = false;
            }
        } catch (final IllegalTypeConversionException | SchemaNotFoundException | MalformedRecordException | IOException e) {
            logger.error("PutAzureCosmoDBRecord failed with error: {}", new Object[]{e.getMessage()}, e);
            error = true;
            yield = false;
        } finally {
            if (yield) {
                context.yield();
            } else if (!error) {
                session.getProvenanceReporter().send(flowFile, getURI(context));
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    @Override
    protected void doPostActionOnSchedule(final ProcessContext context) {
        conflictHandlingStrategy = context.getProperty(CONFLICT_HANDLE_STRATEGY).getValue();
        if (conflictHandlingStrategy == null)
            conflictHandlingStrategy = IGNORE_CONFLICT.getValue();
    }

}
