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
package org.apache.nifi.processors.salesforce;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.NullSuppression;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.OutputGrouping;
import org.apache.nifi.json.WriteJsonResult;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.salesforce.rest.SalesforceConfiguration;
import org.apache.nifi.processors.salesforce.rest.SalesforceRestClient;
import org.apache.nifi.processors.salesforce.util.RecordExtender;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.API_VERSION;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.OLD_API_VERSION_PROPERTY_NAME;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.OLD_READ_TIMEOUT_PROPERTY_NAME;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.OLD_SALESFORCE_INSTANCE_URL_PROPERTY_NAME;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.OLD_TOKEN_PROVIDER_PROPERTY_NAME;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.READ_TIMEOUT;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.SALESFORCE_INSTANCE_URL;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.TOKEN_PROVIDER;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"salesforce", "sobject", "put"})
@CapabilityDescription("Creates new records for the specified Salesforce sObject. The type of the Salesforce object must be set in the input flowfile's"
        + " 'objectType' attribute. This processor cannot update existing records.")
@ReadsAttribute(attribute = "objectType", description = "The Salesforce object type to upload records to. E.g. Account, Contact, Campaign.")
@WritesAttribute(attribute = "error.message", description = "The error message returned by Salesforce.")
@SeeAlso(QuerySalesforceObject.class)
public class PutSalesforceObject extends AbstractProcessor {

    private static final int MAX_RECORD_COUNT = 200;
    private static final String ATTR_OBJECT_TYPE = "objectType";
    private static final String ATTR_ERROR_MESSAGE = "error.message";

    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description(
                    "Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful execution.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("For FlowFiles created as a result of an execution error.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SALESFORCE_INSTANCE_URL,
            API_VERSION,
            READ_TIMEOUT,
            TOKEN_PROVIDER,
            RECORD_READER_FACTORY
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private volatile SalesforceRestClient salesforceRestClient;
    private volatile int maxRecordCount;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        maxRecordCount = getMaxRecordCount();
        final SalesforceConfiguration configuration = createSalesforceConfiguration(context);
        salesforceRestClient = new SalesforceRestClient(configuration);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String objectType = flowFile.getAttribute(ATTR_OBJECT_TYPE);
        if (objectType == null) {
            handleInvalidFlowFile(session, flowFile);
            return;
        }

        try {
            final long startNanos = System.nanoTime();
            processRecords(flowFile, objectType, context, session);
            session.transfer(flowFile, REL_SUCCESS);
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, salesforceRestClient.getVersionedBaseUrl() + "/put/" + objectType, transferMillis);
        } catch (final MalformedRecordException e) {
            getLogger().error("Couldn't read records from input", e);
            transferToFailure(session, flowFile, e.getMessage());
        } catch (final SchemaNotFoundException e) {
            getLogger().error("Couldn't create record writer", e);
            transferToFailure(session, flowFile, e.getMessage());
        } catch (final Exception e) {
            getLogger().error("Failed to put records to Salesforce.", e);
            transferToFailure(session, flowFile, e.getMessage());
        }
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty("record-reader", RECORD_READER_FACTORY.getName());
        config.renameProperty(OLD_SALESFORCE_INSTANCE_URL_PROPERTY_NAME, SALESFORCE_INSTANCE_URL.getName());
        config.renameProperty(OLD_API_VERSION_PROPERTY_NAME, API_VERSION.getName());
        config.renameProperty(OLD_READ_TIMEOUT_PROPERTY_NAME, READ_TIMEOUT.getName());
        config.renameProperty(OLD_TOKEN_PROVIDER_PROPERTY_NAME, TOKEN_PROVIDER.getName());
    }

    private void processRecords(final FlowFile flowFile, final String objectType, final ProcessContext context,
            final ProcessSession session) throws IOException, MalformedRecordException, SchemaNotFoundException {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        int count = 0;
        final RecordExtender recordExtender;

        try (InputStream in = session.read(flowFile);
             RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             WriteJsonResult writer = getWriter(recordExtender = getExtender(reader), out)) {

            Record record;
            while ((record = reader.nextRecord()) != null) {
                count++;
                if (!writer.isActiveRecordSet()) {
                    writer.beginRecordSet();
                }

                final MapRecord extendedRecord = recordExtender.getExtendedRecord(objectType, count, record);
                writer.write(extendedRecord);

                if (count == maxRecordCount) {
                    count = 0;
                    postRecordBatch(objectType, out, writer, recordExtender);
                    out.reset();
                }
            }
            if (writer.isActiveRecordSet()) {
                postRecordBatch(objectType, out, writer, recordExtender);
            }
        }
    }

    private SalesforceConfiguration createSalesforceConfiguration(final ProcessContext context) {
        final String salesforceVersion = context.getProperty(API_VERSION).evaluateAttributeExpressions().getValue();
        final String instanceUrl = context.getProperty(SALESFORCE_INSTANCE_URL).evaluateAttributeExpressions().getValue();
        final OAuth2AccessTokenProvider accessTokenProvider =
                context.getProperty(TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);
        return SalesforceConfiguration.create(instanceUrl, salesforceVersion,
                () -> accessTokenProvider.getAccessDetails().getAccessToken(), 0);
    }

    private void handleInvalidFlowFile(final ProcessSession session, final FlowFile flowFile) {
        final String errorMessage = "Salesforce object type not found among the incoming FlowFile attributes";
        getLogger().error(errorMessage);
        transferToFailure(session, flowFile, errorMessage);
    }

    private void transferToFailure(final ProcessSession session, final FlowFile flowFile, final String message) {
        final FlowFile updatedFlowFile = session.putAttribute(flowFile, ATTR_ERROR_MESSAGE, message);
        session.transfer(session.penalize(updatedFlowFile), REL_FAILURE);
    }

    private void postRecordBatch(final String objectType, final ByteArrayOutputStream out, final WriteJsonResult writer, final RecordExtender extender) throws IOException {
        writer.finishRecordSet();
        writer.flush();
        final ObjectNode wrappedJson = extender.getWrappedRecordsJson(out);
        salesforceRestClient.postRecord(objectType, wrappedJson.toPrettyString());
    }

    private WriteJsonResult getWriter(final RecordExtender extender, final ByteArrayOutputStream out) throws IOException {
        final RecordSchema extendedSchema = extender.getExtendedSchema();
        return new WriteJsonResult(getLogger(), extendedSchema, new NopSchemaAccessWriter(), out,
                true, NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null);
    }

    private RecordExtender getExtender(final RecordReader reader) throws MalformedRecordException {
        return new RecordExtender(reader.getSchema());
    }

    int getMaxRecordCount() {
        return MAX_RECORD_COUNT;
    }
}
