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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.OutputGrouping;
import org.apache.nifi.json.WriteJsonResult;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.salesforce.util.RecordExtender;
import org.apache.nifi.processors.salesforce.util.SalesforceRestService;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"salesforce", "sobject", "put"})
@CapabilityDescription("Posts records to a Salesforce sObject. The type of the Salesforce object must be set in the input flowfile's"
        + " 'objectType' attribute.")
public class PutSalesforceRecord extends AbstractProcessor {

    private static final int MAX_RECORD_COUNT = 200;

    static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("salesforce-url")
            .displayName("URL")
            .description(
                    "The URL for the Salesforce REST API including the domain without additional path information, such as https://MyDomainName.my.salesforce.com")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
            .name("salesforce-api-version")
            .displayName("API Version")
            .description(
                    "The version number of the Salesforce REST API appended to the URL after the services/data path. See Salesforce documentation for supported versions")
            .required(true)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("54.0")
            .build();

    static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("read-timeout")
            .displayName("Read Timeout")
            .description("Maximum time allowed for reading a response from the Salesforce REST API")
            .required(true)
            .defaultValue("15 s")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("oauth2-access-token-provider")
            .displayName("OAuth2 Access Token Provider")
            .description(
                    "Service providing OAuth2 Access Tokens for authenticating using the HTTP Authorization Header")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .build();

    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
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

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            API_URL,
            API_VERSION,
            READ_TIMEOUT,
            TOKEN_PROVIDER,
            RECORD_READER_FACTORY
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    private volatile SalesforceRestService salesforceRestService;
    private volatile int maxRecordCount;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        maxRecordCount = getMaxRecordCount();

        String salesforceVersion = context.getProperty(API_VERSION).getValue();
        String baseUrl = context.getProperty(API_URL).getValue();
        OAuth2AccessTokenProvider accessTokenProvider =
                context.getProperty(TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);

        salesforceRestService = new SalesforceRestService(
                salesforceVersion,
                baseUrl,
                () -> accessTokenProvider.getAccessDetails().getAccessToken(),
                context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS)
                        .intValue()
        );
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String objectType = flowFile.getAttribute("objectType");
        if (objectType == null) {
            throw new ProcessException("Salesforce object type not found");
        }

        RecordReaderFactory readerFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        RecordExtender extender = new RecordExtender();

        try (InputStream in = session.read(flowFile);
             RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             WriteJsonResult writer = getWriter(reader.getSchema(), extender, out)) {

            AtomicInteger count = new AtomicInteger();
            RecordSchema originalSchema = reader.getSchema();
            Record record;

            while ((record = reader.nextRecord()) != null) {
                count.incrementAndGet();
                if (!writer.isActiveRecordSet()) {
                    writer.beginRecordSet();
                }

                MapRecord extendedRecord = extender.getExtendedRecord(objectType, originalSchema, count.get(), record);
                writer.write(extendedRecord);

                if (count.compareAndSet(maxRecordCount, 0)) {
                    processRecords(objectType, out, writer, extender);
                    out.reset();
                }
            }

            if (writer.isActiveRecordSet()) {
                processRecords(objectType, out, writer, extender);
            }

        } catch (Exception ex) {
            getLogger().error("Failed to put records to Salesforce.", ex);
            session.transfer(flowFile, REL_FAILURE);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    private void processRecords(String objectType, ByteArrayOutputStream out, WriteJsonResult writer, RecordExtender extender) throws IOException {
        writer.finishRecordSet();
        writer.flush();
        ObjectNode wrappedJson = extender.getWrappedRecordsJson(out);
        salesforceRestService.postRecord(objectType, wrappedJson.toPrettyString());
    }

    private WriteJsonResult getWriter(RecordSchema originalSchema, RecordExtender extender, ByteArrayOutputStream out) throws IOException {
        final SimpleRecordSchema extendedSchema = extender.getExtendedSchema(originalSchema);
        return new WriteJsonResult(getLogger(), extendedSchema, new NopSchemaAccessWriter(), out,
                true, NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null);
    }

    int getMaxRecordCount() {
        return MAX_RECORD_COUNT;
    }
}
