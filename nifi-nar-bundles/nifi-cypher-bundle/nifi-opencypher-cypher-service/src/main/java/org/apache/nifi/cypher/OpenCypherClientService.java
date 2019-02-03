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

package org.apache.nifi.cypher;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.opencypher.gremlin.neo4j.driver.GremlinDatabase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenCypherClientService extends AbstractControllerService implements CypherClientService {
    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
        .name("opencypher-contact-points")
        .displayName("Contact Points")
        .description("A comma-separated list of hostnames or IP addresses where an OpenCypher-enabled server can be found.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        CONTACT_POINTS
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private volatile Driver gremlinDriver;
    private volatile String transitUrl;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        String contactProp = context.getProperty(CONTACT_POINTS).getValue();
        String[] contactPoints = contactProp.split(",[\\s]*");
        Cluster.Builder builder = Cluster.build();
        for (String contactPoint : contactPoints) {
            builder.addContactPoint(contactPoint.trim());
        }
        builder.enableSsl(false);
        Cluster cluster = builder.create();

        gremlinDriver = GremlinDatabase.driver(cluster);
        transitUrl = String.format("gremlin://%s", contactProp);
    }

    @OnDisabled
    public void onDisabled() {
        gremlinDriver.close();
    }

    public static final String NOT_SUPPORTED = "NOT_SUPPORTED";

    @Override
    public Map<String, String> executeQuery(String query, Map<String, Object> parameters, CypherQueryResultCallback handler) {
        try (Session session = gremlinDriver.session()) {
            StatementResult result = session.run(query, parameters);
            long count = 0;
            while (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> asMap = record.asMap();
                handler.process(asMap, result.hasNext());
                count++;
            }

            Map<String,String> resultAttributes = new HashMap<>();
            resultAttributes.put(NODES_CREATED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_CREATED, NOT_SUPPORTED);
            resultAttributes.put(LABELS_ADDED, NOT_SUPPORTED);
            resultAttributes.put(NODES_DELETED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_DELETED, NOT_SUPPORTED);
            resultAttributes.put(PROPERTIES_SET, NOT_SUPPORTED);
            resultAttributes.put(ROWS_RETURNED, String.valueOf(count));

            return resultAttributes;
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }

    @Override
    public String getTransitUrl() {
        return transitUrl;
    }
}
