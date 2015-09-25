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
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.couchbase.client.java.Bucket;

/**
 * Provides common functionalities for Couchbase processors.
 */
public abstract class AbstractCouchbaseProcessor extends AbstractProcessor {

    public static final PropertyDescriptor DOCUMENT_TYPE = new PropertyDescriptor
            .Builder().name("Document Type")
            .description("The type of contents.")
            .required(true)
            .allowableValues(DocumentType.values())
            .defaultValue(DocumentType.Json.toString())
            .build();

    public static final PropertyDescriptor DOC_ID = new PropertyDescriptor
            .Builder().name("Static Document Id")
            .description("A static, fixed Couchbase document id.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DOC_ID_EXP = new PropertyDescriptor
            .Builder().name("Document Id Expression")
            .description("An expression to construct the Couchbase document id."
                    + " If 'Static Document Id' is specified, then 'Static Document Id' is used.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are written to Couchbase Server are routed to this relationship.")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input file will be routed to this destination when it has been successfully processed.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot written to Couchbase Server are routed to this relationship.")
            .build();

    public static final PropertyDescriptor COUCHBASE_CLUSTER_SERVICE = new PropertyDescriptor
            .Builder().name("Couchbase Cluster Controller Service")
            .description("A Couchbase Cluster Controller Service which manages connections to a Couchbase cluster.")
            .required(true)
            .identifiesControllerService(CouchbaseClusterControllerService.class)
            .build();

    public static final PropertyDescriptor BUCKET_NAME = new PropertyDescriptor
            .Builder().name("Bucket Name")
            .description("The name of bucket to access.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("default")
            .build();

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
     * @param descriptors add properties to this list
     */
    protected void addSupportedProperties(List<PropertyDescriptor> descriptors) {
        return;
    }

    /**
     * Add processor specific relationships.
     * @param relationships add relationships to this list
     */
    protected void addSupportedRelationships(Set<Relationship> relationships) {
        return;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private CouchbaseClusterControllerService getClusterService(final ProcessContext context) {
        if(clusterService == null){
            synchronized(AbstractCouchbaseProcessor.class){
                if(clusterService == null){
                    clusterService = context.getProperty(COUCHBASE_CLUSTER_SERVICE)
                            .asControllerService(CouchbaseClusterControllerService.class);
                }
            }
        }

        return clusterService;
    }

    /**
     * Open a bucket connection using a CouchbaseClusterControllerService.
     * @param context a process context
     * @return a bucket instance
     */
    protected final Bucket openBucket(final ProcessContext context) {
        return getClusterService(context).openBucket(context.getProperty(BUCKET_NAME).getValue());
    }

    /**
     * Generate a transit url.
     * @param context a process context
     * @return a transit url based on the bucket name and the CouchbaseClusterControllerService name
     */
    protected String getTransitUrl(final ProcessContext context) {
        return new StringBuilder(context.getProperty(BUCKET_NAME).getValue())
            .append('@')
            .append(context.getProperty(COUCHBASE_CLUSTER_SERVICE).getValue())
            .toString();
    }

}
