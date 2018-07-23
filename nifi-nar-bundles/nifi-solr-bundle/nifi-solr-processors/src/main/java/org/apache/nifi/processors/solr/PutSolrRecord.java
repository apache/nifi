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
package org.apache.nifi.processors.solr;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.processors.solr.SolrUtils.BASIC_PASSWORD;
import static org.apache.nifi.processors.solr.SolrUtils.BASIC_USERNAME;
import static org.apache.nifi.processors.solr.SolrUtils.COLLECTION;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_LOCATION;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_MAX_CONNECTIONS;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_MAX_CONNECTIONS_PER_HOST;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_SOCKET_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE_CLOUD;
import static org.apache.nifi.processors.solr.SolrUtils.SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.ZK_CLIENT_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.ZK_CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.writeRecord;


@Tags({"Apache", "Solr", "Put", "Send","Record"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Indexes the Records from a FlowFile into Solr")
@DynamicProperty(name="A Solr request parameter name", value="A Solr request parameter value",
        description="These parameters will be passed to Solr on the request")
public class PutSolrRecord extends SolrProcessor {

    public static final PropertyDescriptor UPDATE_PATH = new PropertyDescriptor
            .Builder().name("Solr Update Path").displayName("Solr Update Path")
            .description("The path in Solr to post the Flowfile Records")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("/update")
            .build();

    public static final PropertyDescriptor FIELDS_TO_INDEX  = new PropertyDescriptor
            .Builder().name("Fields To Index").displayName("Fields To Index")
            .displayName("Fields To Index")
            .description("Comma-separated list of field names to write")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor COMMIT_WITHIN = new PropertyDescriptor
            .Builder().name("Commit Within").displayName("Commit Within")
            .description("The number of milliseconds before the given update is committed")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("5000")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("Batch Size").displayName("Batch Size")
            .description("The number of solr documents to index per batch")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("500")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The original FlowFile")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed for any reason other than Solr being unreachable")
            .build();

    public static final Relationship REL_CONNECTION_FAILURE = new Relationship.Builder()
            .name("connection_failure")
            .description("FlowFiles that failed because Solr is unreachable")
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("put-solr-record-record-reader").displayName("put-solr-record-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final String COLLECTION_PARAM_NAME = "collection";
    public static final String COMMIT_WITHIN_PARAM_NAME = "commitWithin";
    public static final String REPEATING_PARAM_PATTERN = "\\w+\\.\\d+";

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private static final String EMPTY_STRING = "";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(COLLECTION);
        descriptors.add(UPDATE_PATH);
        descriptors.add(RECORD_READER);
        descriptors.add(FIELDS_TO_INDEX);
        descriptors.add(COMMIT_WITHIN);
        descriptors.add(KERBEROS_CREDENTIALS_SERVICE);
        descriptors.add(BASIC_USERNAME);
        descriptors.add(BASIC_PASSWORD);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(SOLR_SOCKET_TIMEOUT);
        descriptors.add(SOLR_CONNECTION_TIMEOUT);
        descriptors.add(SOLR_MAX_CONNECTIONS);
        descriptors.add(SOLR_MAX_CONNECTIONS_PER_HOST);
        descriptors.add(ZK_CLIENT_TIMEOUT);
        descriptors.add(ZK_CONNECTION_TIMEOUT);
        descriptors.add(BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_CONNECTION_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value to send for the '" + propertyDescriptorName + "' request parameter")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public void doOnTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final AtomicReference<Exception> error = new AtomicReference<>(null);
        final AtomicReference<Exception> connectionError = new AtomicReference<>(null);

        final boolean isSolrCloud = SOLR_TYPE_CLOUD.getValue().equals(context.getProperty(SOLR_TYPE).getValue());
        final String collection = context.getProperty(COLLECTION).evaluateAttributeExpressions(flowFile).getValue();
        final Long commitWithin = context.getProperty(COMMIT_WITHIN).evaluateAttributeExpressions(flowFile).asLong();
        final String contentStreamPath = context.getProperty(UPDATE_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final MultiMapSolrParams requestParams = new MultiMapSolrParams(SolrUtils.getRequestParams(context, flowFile));
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String fieldsToIndex = context.getProperty(FIELDS_TO_INDEX).evaluateAttributeExpressions(flowFile).getValue();
        final Long batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions(flowFile).asLong();

        final List<String> fieldList = new ArrayList<>();
        if (!StringUtils.isBlank(fieldsToIndex)) {
            Arrays.stream(fieldsToIndex.split(",")).forEach( f -> fieldList.add(f.trim()));
        }
        StopWatch timer = new StopWatch(true);
        try (final InputStream in = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

            Record record;
            List<SolrInputDocument> inputDocumentList = new LinkedList<>();
            try {
                while ((record = reader.nextRecord()) != null) {
                SolrInputDocument inputDoc = new SolrInputDocument();
                writeRecord(record, inputDoc,fieldList,EMPTY_STRING);
                inputDocumentList.add(inputDoc);
                if(inputDocumentList.size()==batchSize) {
                    index(isSolrCloud, collection, commitWithin, contentStreamPath, requestParams, inputDocumentList);
                    inputDocumentList = new ArrayList<>();
                }
               index(isSolrCloud, collection, commitWithin, contentStreamPath, requestParams, inputDocumentList);
            }
            } catch (SolrException e) {
                error.set(e);
            } catch (SolrServerException e) {
                if (causedByIOException(e)) {
                    //Exit in case of a solr connection error
                    connectionError.set(e);
                } else {
                    error.set(e);
                }
            } catch (IOException e) {
                //Exit in case of a solr connection error
                connectionError.set(e);
            }
        } catch (final IOException | SchemaNotFoundException | MalformedRecordException e) {
            getLogger().error("Could not parse incoming data", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        timer.stop();

        if (error.get() != null) {
            getLogger().error("Failed to send all the records of the {} to Solr due to {}; routing to failure",
                    new Object[]{flowFile, error.get()});
            session.transfer(flowFile, REL_FAILURE);
        } else if (connectionError.get() != null) {
            getLogger().error("Failed to send {} to Solr due to {}; routing to connection_failure",
                    new Object[]{flowFile, connectionError.get()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_CONNECTION_FAILURE);
        } else {
            StringBuilder transitUri = new StringBuilder("solr://");
            transitUri.append(getSolrLocation());
            if (isSolrCloud) {
                transitUri.append(":").append(collection);
            }

            final long duration = timer.getDuration(TimeUnit.MILLISECONDS);
            session.getProvenanceReporter().send(flowFile, transitUri.toString(), duration, true);
            getLogger().info("Successfully sent {} to Solr in {} millis", new Object[]{flowFile, duration});
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private void index(boolean isSolrCloud, String collection, Long commitWithin, String contentStreamPath, MultiMapSolrParams requestParams, List<SolrInputDocument> inputDocumentList)
            throws IOException, SolrServerException,SolrException {
        UpdateRequest request = new UpdateRequest(contentStreamPath);
        request.setParams(new ModifiableSolrParams());

        // add the extra params, don't use 'set' in case of repeating params
        Iterator<String> paramNames = requestParams.getParameterNamesIterator();
        while (paramNames.hasNext()) {
            String paramName = paramNames.next();
            for (String paramValue : requestParams.getParams(paramName)) {
                request.getParams().add(paramName, paramValue);
            }
        }

        // specify the collection for SolrCloud
        if (isSolrCloud) {
            request.setParam(COLLECTION_PARAM_NAME, collection);
        }

        if (commitWithin != null && commitWithin > 0) {
            request.setParam(COMMIT_WITHIN_PARAM_NAME, commitWithin.toString());
        }

        // if a username and password were provided then pass them for basic auth
        if (isBasicAuthEnabled()) {
            request.setBasicAuthCredentials(getUsername(), getPassword());
        }
        request.add(inputDocumentList);
        UpdateResponse response = request.process(getSolrClient());
        getLogger().debug("Got {} response from Solr", new Object[]{response.getStatus()});
        inputDocumentList.clear();
    }

    private boolean causedByIOException(SolrServerException e) {
        boolean foundIOException = false;
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof IOException) {
                foundIOException = true;
                break;
            }
            cause = cause.getCause();
        }
        return foundIOException;
    }

}
