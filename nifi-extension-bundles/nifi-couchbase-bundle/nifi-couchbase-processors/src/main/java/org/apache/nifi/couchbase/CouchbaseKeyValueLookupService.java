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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.reporting.InitializationException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.LOOKUP_SUB_DOC_PATH;

@Tags({"lookup", "enrich", "key", "value", "couchbase"})
@CapabilityDescription("Lookup a string value from Couchbase Server associated with the specified key."
        + " The coordinates that are passed to the lookup must contain the key 'key'.")
public class CouchbaseKeyValueLookupService extends AbstractCouchbaseLookupService implements StringLookupService {

    private volatile String subDocPath;

    @Override
    protected void addProperties(List<PropertyDescriptor> properties) {
        properties.add(LOOKUP_SUB_DOC_PATH);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        subDocPath = context.getProperty(LOOKUP_SUB_DOC_PATH).evaluateAttributeExpressions().getValue();
    }

    @Override
    public Optional<String> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        try {
            final Bucket bucket = couchbaseClusterService.openBucket(bucketName);
            final Scope scope = bucket.scope(scopeName);
            final Collection collection = scope.collection(collectionName);
            final Optional<String> docId = Optional.ofNullable(coordinates.get(KEY)).map(Object::toString);
            if (subDocPath != null && !subDocPath.isBlank()) {
                return docId.map(key -> {
                    try {
                        return CouchbaseUtils.getSubDocPath(collection.get(key).contentAsObject(), subDocPath);
                    } catch (DocumentNotFoundException e) {
                        getLogger().debug("Document was not found for {}", key);
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                return docId.map(key -> CouchbaseUtils.getStringContent(collection, key));
            }
        } catch (CouchbaseException e) {
            throw new LookupFailureException("Failed to lookup from Couchbase using this coordinates: " + coordinates);
        } catch (Exception e) {
            throw new LookupFailureException("Failed to lookup from Couchbase using this coordinates: " + coordinates, e);
        }
    }
}
