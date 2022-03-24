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

package org.apache.nifi.graph;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class AbstractTinkerpopClientService extends AbstractControllerService {
    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("tinkerpop-contact-points")
            .displayName("Contact Points")
            .description("A comma-separated list of hostnames or IP addresses where an OpenCypher-enabled server can be found.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("tinkerpop-port")
            .displayName("Port")
            .description("The port where Gremlin Server is running on each host listed as a contact point.")
            .required(true)
            .defaultValue("8182")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
            .name("tinkerpop-path")
            .displayName("Path")
            .description("The URL path where Gremlin Server is running on each host listed as a contact point.")
            .required(true)
            .defaultValue("/gremlin")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("tinkerpop-ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            CONTACT_POINTS, PORT, PATH, SSL_CONTEXT_SERVICE
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    protected Cluster.Builder setupSSL(ConfigurationContext context, Cluster.Builder builder) {
        if (context.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            SSLContextService service = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            builder
                    .enableSsl(true)
                    .keyStore(service.getKeyStoreFile())
                    .keyStorePassword(service.getKeyStorePassword())
                    .keyStoreType(service.getKeyStoreType())
                    .trustStore(service.getTrustStoreFile())
                    .trustStorePassword(service.getTrustStorePassword());
            usesSSL = true;
        }

        return builder;
    }

    boolean usesSSL;
    protected String transitUrl;

    protected Cluster buildCluster(ConfigurationContext context) {
        String contactProp = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        String path = context.getProperty(PATH).evaluateAttributeExpressions().getValue();
        String[] contactPoints = contactProp.split(",[\\s]*");
        Cluster.Builder builder = Cluster.build();
        for (String contactPoint : contactPoints) {
            builder.addContactPoint(contactPoint.trim());
        }

        builder.port(port).path(path);

        builder = setupSSL(context, builder);

        transitUrl = String.format("gremlin%s://%s:%s%s", usesSSL ? "+ssl" : "",
                contactProp, port, path);

        return builder.create();
    }
}
