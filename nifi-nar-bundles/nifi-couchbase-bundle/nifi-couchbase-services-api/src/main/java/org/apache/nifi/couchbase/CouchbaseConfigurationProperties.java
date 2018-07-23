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
package org.apache.nifi.couchbase;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;

public class CouchbaseConfigurationProperties {

    public static final PropertyDescriptor COUCHBASE_CLUSTER_SERVICE = new PropertyDescriptor.Builder()
            .name("cluster-controller-service")
            .displayName("Couchbase Cluster Controller Service")
            .description("A Couchbase Cluster Controller Service which manages connections to a Couchbase cluster.")
            .required(true)
            .identifiesControllerService(CouchbaseClusterControllerService.class)
            .build();

    public static final PropertyDescriptor BUCKET_NAME = new PropertyDescriptor.Builder()
            .name("bucket-name")
            .displayName("Bucket Name")
            .description("The name of bucket to access.")
            .required(true)
            .addValidator(Validator.VALID)
            .defaultValue("default")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DOCUMENT_TYPE = new PropertyDescriptor.Builder()
            .name("document-type")
            .displayName("Document Type")
            .description("The type of contents.")
            .required(true)
            .allowableValues(DocumentType.values())
            .defaultValue(DocumentType.Json.toString())
            .build();

    public static final PropertyDescriptor LOOKUP_SUB_DOC_PATH = new PropertyDescriptor.Builder()
            .name("lookup-sub-doc-path")
            .displayName("Lookup Sub-Document Path")
            .description("The Sub-Document lookup path within the target JSON document.")
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
}
