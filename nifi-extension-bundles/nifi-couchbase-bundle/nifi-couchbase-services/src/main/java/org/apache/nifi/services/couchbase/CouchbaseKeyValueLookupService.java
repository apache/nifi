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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.couchbase.exception.CouchbaseDocNotFoundException;
import org.apache.nifi.services.couchbase.utils.CouchbaseLookupInResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Tags({"lookup", "enrich", "key", "value", "couchbase"})
@CapabilityDescription("Lookup a string value from Couchbase Server associated with the specified key. The coordinates that are passed to the lookup must contain the key 'key'.")
public class CouchbaseKeyValueLookupService extends AbstractCouchbaseService implements StringLookupService {

    public static final PropertyDescriptor LOOKUP_SUB_DOC_PATH = new PropertyDescriptor.Builder()
            .name("Lookup Sub-Document Path")
            .description("The Sub-Document lookup path within the target JSON document.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            COUCHBASE_CONNECTION_SERVICE,
            BUCKET_NAME,
            SCOPE_NAME,
            COLLECTION_NAME,
            LOOKUP_SUB_DOC_PATH
    );

    private volatile String subDocPath;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        super.onEnabled(context);
        subDocPath = context.getProperty(LOOKUP_SUB_DOC_PATH).evaluateAttributeExpressions().getValue();
    }

    @Override
    public Optional<String> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        final Object documentId = coordinates.get(KEY);

        if (documentId == null) {
            return Optional.empty();
        }

        try {
            final CouchbaseLookupInResult result = couchbaseClient.lookupIn(documentId.toString(), subDocPath);
            return Optional.ofNullable(result.resultContent()).map(Object::toString);
        } catch (CouchbaseDocNotFoundException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw new LookupFailureException("Key-value lookup from Couchbase failed", e);
        }
    }
}
