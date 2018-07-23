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
package org.apache.nifi.processors.couchbase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.couchbase.CouchbaseClusterControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;

/**
 * Provides common functionality for Couchbase processors.
 */
public abstract class AbstractCouchbaseProcessor extends AbstractProcessor {

    static final PropertyDescriptor DOC_ID = new PropertyDescriptor.Builder()
        .name("document-id")
        .displayName("Document Id")
        .description("A static, fixed Couchbase document id, or an expression to construct the Couchbase document id.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();


    static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original").build();
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
    static final Relationship REL_RETRY = new Relationship.Builder().name("retry").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private CouchbaseClusterControllerService clusterService;

    @Override
    protected final void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(COUCHBASE_CLUSTER_SERVICE);
        descriptors.add(BUCKET_NAME);
        addSupportedProperties(descriptors);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        addSupportedRelationships(relationships);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    /**
     * Add processor specific properties.
     *
     * @param descriptors add properties to this list
     */
    protected void addSupportedProperties(List<PropertyDescriptor> descriptors) {
        return;
    }

    /**
     * Add processor specific relationships.
     *
     * @param relationships add relationships to this list
     */
    protected void addSupportedRelationships(Set<Relationship> relationships) {
        return;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return filterRelationships(this.relationships);
    }

    protected Set<Relationship> filterRelationships(Set<Relationship> rels) {
        return rels;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private CouchbaseClusterControllerService getClusterService(final ProcessContext context) {
        synchronized (AbstractCouchbaseProcessor.class) {
            if (clusterService == null) {
                clusterService = context.getProperty(COUCHBASE_CLUSTER_SERVICE)
                        .asControllerService(CouchbaseClusterControllerService.class);
            }
        }

        return clusterService;
    }

    /**
     * Open a bucket connection using a CouchbaseClusterControllerService.
     *
     * @param context a process context
     * @return a bucket instance
     */
    protected final Bucket openBucket(final ProcessContext context) {
        return getClusterService(context).openBucket(context.getProperty(BUCKET_NAME).evaluateAttributeExpressions().getValue());
    }

    /**
     * Generate a transit url.
     *
     * @param bucket the target bucket
     * @return a transit url based on the bucket name and the CouchbaseClusterControllerService name
     */
    protected String getTransitUrl(final Bucket bucket, final String docId) {
        return "couchbase://" + bucket.name() + "/" + docId;
    }

    /**
     * Handles the thrown CouchbaseException accordingly.
     *
     * @param context a process context
     * @param session a process session
     * @param logger a logger
     * @param inFile an input FlowFile
     * @param e the thrown CouchbaseException
     * @param errMsg a message to be logged
     */
    protected void handleCouchbaseException(final ProcessContext context, final ProcessSession session,
        final ComponentLog logger, FlowFile inFile, CouchbaseException e,
        String errMsg) {
        logger.error(errMsg, e);
        if (inFile != null) {
            ErrorHandlingStrategy strategy = CouchbaseExceptionMappings.getStrategy(e);
            switch (strategy.penalty()) {
                case Penalize:
                    if (logger.isDebugEnabled()) {
                        logger.debug("Penalized: {}", new Object[] {inFile});
                    }
                    inFile = session.penalize(inFile);
                    break;
                case Yield:
                    if (logger.isDebugEnabled()) {
                        logger.debug("Yielded context: {}", new Object[] {inFile});
                    }
                    context.yield();
                    break;
                case None:
                    break;
            }

            switch (strategy.result()) {
                case ProcessException:
                    throw new ProcessException(errMsg, e);
                case Failure:
                    inFile = session.putAttribute(inFile, CouchbaseAttributes.Exception.key(), e.getClass().getName());
                    session.transfer(inFile, REL_FAILURE);
                    break;
                case Retry:
                    inFile = session.putAttribute(inFile, CouchbaseAttributes.Exception.key(), e.getClass().getName());
                    session.transfer(inFile, REL_RETRY);
                    break;
            }
        }
    }
}
