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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;

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
            final Optional<String> docId = Optional.ofNullable(coordinates.get(KEY)).map(Object::toString);

            if (!StringUtils.isBlank(subDocPath)) {
                return docId.map(key -> {
                    try {
                        return bucket.lookupIn(key).get(subDocPath).execute();
                    } catch (DocumentDoesNotExistException e) {
                        getLogger().debug("Document was not found for {}", new Object[]{key});
                        return null;
                    }
                }).map(fragment -> fragment.content(0)).map(Object::toString);

            } else {
                return docId.map(key -> CouchbaseUtils.getStringContent(bucket, key));
            }
        } catch (CouchbaseException e) {
            throw new LookupFailureException("Failed to lookup from Couchbase using this coordinates: " + coordinates);
        }


    }

}
