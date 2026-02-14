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
package org.apache.nifi.services.couchbase;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.couchbase.utils.CouchbaseContext;
import org.apache.nifi.services.couchbase.utils.DocumentType;

import java.util.Set;

public abstract class AbstractCouchbaseService extends AbstractControllerService {

    protected static final String KEY = "key";
    protected static final Set<String> REQUIRED_KEYS = Set.of(KEY);

    private static final String DEFAULT_BUCKET = "default";
    private static final String DEFAULT_SCOPE = "_default";
    private static final String DEFAULT_COLLECTION = "_default";

    public static final PropertyDescriptor COUCHBASE_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Couchbase Connection Service")
            .description("A Couchbase Connection Service which manages connections to a Couchbase cluster.")
            .required(true)
            .identifiesControllerService(CouchbaseConnectionService.class)
            .build();

    public static final PropertyDescriptor BUCKET_NAME = new PropertyDescriptor.Builder()
            .name("Bucket Name")
            .description("The name of the bucket where documents will be stored. Each bucket contains a hierarchy of scopes and collections to group keys and values logically.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(DEFAULT_BUCKET)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SCOPE_NAME = new PropertyDescriptor.Builder()
            .name("Scope Name")
            .description("The name of the scope  which is a logical namespace within a bucket, serving to categorize and organize related collections.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(DEFAULT_SCOPE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("Collection Name")
            .description("The name of collection which is a logical container within a scope, used to hold documents.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(DEFAULT_COLLECTION)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    protected volatile CouchbaseClient couchbaseClient;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final CouchbaseConnectionService connectionService = context.getProperty(COUCHBASE_CONNECTION_SERVICE).asControllerService(CouchbaseConnectionService.class);
        final CouchbaseContext couchbaseContext = getCouchbaseContext(context);
        couchbaseClient = connectionService.getClient(couchbaseContext);
    }

    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    private CouchbaseContext getCouchbaseContext(ConfigurationContext context) {
        final String bucketName = context.getProperty(BUCKET_NAME).evaluateAttributeExpressions().getValue();
        final String scopeName = context.getProperty(SCOPE_NAME).evaluateAttributeExpressions().getValue();
        final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions().getValue();

        return new CouchbaseContext(bucketName, scopeName, collectionName, DocumentType.BINARY);
    }
}
