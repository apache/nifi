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

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;

public class AbstractCouchbaseLookupService extends AbstractControllerService {

    protected static final String KEY = "key";
    protected static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY).collect(Collectors.toSet()));

    protected List<PropertyDescriptor> properties;
    protected volatile CouchbaseClusterControllerService couchbaseClusterService;
    protected volatile String bucketName;

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(COUCHBASE_CLUSTER_SERVICE);
        properties.add(BUCKET_NAME);
        addProperties(properties);
        this.properties = Collections.unmodifiableList(properties);
    }

    protected void addProperties(List<PropertyDescriptor> properties) {
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        couchbaseClusterService = context.getProperty(COUCHBASE_CLUSTER_SERVICE)
                .asControllerService(CouchbaseClusterControllerService.class);
        bucketName = context.getProperty(BUCKET_NAME).evaluateAttributeExpressions().getValue();
    }

    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


}
