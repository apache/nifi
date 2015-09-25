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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;

/**
 * Provides a centralized Couchbase connection and bucket passwords management.
 */
@CapabilityDescription("Provides a centralized Couchbase connection and bucket passwords management."
        + " Bucket passwords can be specified via dynamic properties.")
@Tags({ "nosql", "couchbase", "database", "connection" })
@DynamicProperty(name = "Bucket Password for BUCKET_NAME", value = "bucket password", description = "Specify bucket password if neseccery.")
public class CouchbaseClusterService extends AbstractControllerService implements CouchbaseClusterControllerService {

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor
            .Builder().name("Connection String")
            .description("The hostnames or ip addresses of the bootstraping nodes and optional parameters."
                    + " Syntax) couchbase://node1,node2,nodeN?param1=value1&param2=value2&paramN=valueN")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CONNECTION_STRING);

        properties = Collections.unmodifiableList(props);
    }

    private static final String DYNAMIC_PROP_BUCKET_PASSWORD = "Bucket Password for ";
    private static final Map<String, String> bucketPasswords = new HashMap<>();

    private volatile CouchbaseCluster cluster;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(
            String propertyDescriptorName) {
        if(propertyDescriptorName.startsWith(DYNAMIC_PROP_BUCKET_PASSWORD)){
            return new PropertyDescriptor
                    .Builder().name(propertyDescriptorName)
                    .description("Bucket password.")
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .dynamic(true)
                    .sensitive(true)
                    .build();
        }
        return null;
    }


    /**
     * Establish a connection to a Couchbase cluster.
     * @param context the configuration context
     * @throws InitializationException if unable to connect a Couchbase cluster
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        for(PropertyDescriptor p : context.getProperties().keySet()){
            if(p.isDynamic() && p.getName().startsWith(DYNAMIC_PROP_BUCKET_PASSWORD)){
                String bucketName = p.getName().substring(DYNAMIC_PROP_BUCKET_PASSWORD.length());
                String password = context.getProperty(p).getValue();
                bucketPasswords.put(bucketName, password);
            }
        }
        try {
            cluster = CouchbaseCluster.fromConnectionString(context.getProperty(CONNECTION_STRING).getValue());
        } catch(CouchbaseException e) {
            throw new InitializationException(e);
        }
    }

    @Override
    public Bucket openBucket(String bucketName){
        return cluster.openBucket(bucketName, bucketPasswords.get(bucketName));
    }

    /**
     * Disconnect from the Couchbase cluster.
     */
    @OnDisabled
    public void shutdown() {
        if(cluster != null){
            cluster.disconnect();
            cluster = null;
        }
    }

}
