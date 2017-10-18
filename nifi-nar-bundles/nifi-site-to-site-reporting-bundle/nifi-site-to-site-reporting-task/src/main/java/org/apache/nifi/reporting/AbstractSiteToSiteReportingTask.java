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
package org.apache.nifi.reporting;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for ReportingTasks that send data over site-to-site.
 */
public abstract class AbstractSiteToSiteReportingTask extends AbstractReportingTask {
    protected static final String DESTINATION_URL_PATH = "/nifi";

    static final PropertyDescriptor DESTINATION_URL = new PropertyDescriptor.Builder()
            .name("Destination URL")
            .displayName("Destination URL")
            .description("The URL of the destination NiFi instance to send data to, " +
                    "should be in the format http(s)://host:port/nifi.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(new NiFiUrlValidator())
            .build();
    static final PropertyDescriptor PORT_NAME = new PropertyDescriptor.Builder()
            .name("Input Port Name")
            .displayName("Input Port Name")
            .description("The name of the Input Port to deliver data to.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service to use when communicating with the destination. If not specified, communications will not be secure.")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();
    static final PropertyDescriptor INSTANCE_URL = new PropertyDescriptor.Builder()
            .name("Instance URL")
            .displayName("Instance URL")
            .description("The URL of this instance to use in the Content URI of each event.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://${hostname(true)}:8080/nifi")
            .addValidator(new NiFiUrlValidator())
            .build();
    static final PropertyDescriptor COMPRESS = new PropertyDescriptor.Builder()
            .name("Compress Events")
            .displayName("Compress Events")
            .description("Indicates whether or not to compress the data being sent.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .displayName("Communications Timeout")
            .description("Specifies how long to wait to a response from the destination before deciding that an error has occurred and canceling the transaction")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("Specifies how many records to send in a single batch, at most.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor TRANSPORT_PROTOCOL = new PropertyDescriptor.Builder()
            .name("s2s-transport-protocol")
            .displayName("Transport Protocol")
            .description("Specifies which transport protocol to use for Site-to-Site communication.")
            .required(true)
            .allowableValues(SiteToSiteTransportProtocol.values())
            .defaultValue(SiteToSiteTransportProtocol.RAW.name())
            .build();
    static final PropertyDescriptor HTTP_PROXY_HOSTNAME = new PropertyDescriptor.Builder()
            .name("s2s-http-proxy-hostname")
            .displayName("HTTP Proxy hostname")
            .description("Specify the proxy server's hostname to use. If not specified, HTTP traffics are sent directly to the target NiFi instance.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    static final PropertyDescriptor HTTP_PROXY_PORT = new PropertyDescriptor.Builder()
            .name("s2s-http-proxy-port")
            .displayName("HTTP Proxy port")
            .description("Specify the proxy server's port number, optional. If not specified, default port 80 will be used.")
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    static final PropertyDescriptor HTTP_PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("s2s-http-proxy-username")
            .displayName("HTTP Proxy username")
            .description("Specify an user name to connect to the proxy server, optional.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    static final PropertyDescriptor HTTP_PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("s2s-http-proxy-password")
            .displayName("HTTP Proxy password")
            .description("Specify an user password to connect to the proxy server, optional.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected volatile SiteToSiteClient siteToSiteClient;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION_URL);
        properties.add(PORT_NAME);
        properties.add(SSL_CONTEXT);
        properties.add(INSTANCE_URL);
        properties.add(COMPRESS);
        properties.add(TIMEOUT);
        properties.add(BATCH_SIZE);
        properties.add(TRANSPORT_PROTOCOL);
        properties.add(HTTP_PROXY_HOSTNAME);
        properties.add(HTTP_PROXY_PORT);
        properties.add(HTTP_PROXY_USERNAME);
        properties.add(HTTP_PROXY_PASSWORD);
        return properties;
    }

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslContextService == null ? null : sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
        final ComponentLog logger = getLogger();
        final EventReporter eventReporter = new EventReporter() {
            @Override
            public void reportEvent(final Severity severity, final String category, final String message) {
                switch (severity) {
                    case WARNING:
                        logger.warn(message);
                        break;
                    case ERROR:
                        logger.error(message);
                        break;
                    default:
                        break;
                }
            }
        };

        final String destinationUrl = context.getProperty(DESTINATION_URL).evaluateAttributeExpressions().getValue();

        final SiteToSiteTransportProtocol mode = SiteToSiteTransportProtocol.valueOf(context.getProperty(TRANSPORT_PROTOCOL).getValue());
        final HttpProxy httpProxy = mode.equals(SiteToSiteTransportProtocol.RAW) || StringUtils.isEmpty(context.getProperty(HTTP_PROXY_HOSTNAME).getValue()) ? null
                : new HttpProxy(context.getProperty(HTTP_PROXY_HOSTNAME).getValue(), context.getProperty(HTTP_PROXY_PORT).asInteger(),
                context.getProperty(HTTP_PROXY_USERNAME).getValue(), context.getProperty(HTTP_PROXY_PASSWORD).getValue());

        siteToSiteClient = new SiteToSiteClient.Builder()
                .url(destinationUrl)
                .portName(context.getProperty(PORT_NAME).getValue())
                .useCompression(context.getProperty(COMPRESS).asBoolean())
                .eventReporter(eventReporter)
                .sslContext(sslContext)
                .timeout(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .transportProtocol(mode)
                .httpProxy(httpProxy)
                .build();
    }

    @OnStopped
    public void shutdown() throws IOException {
        final SiteToSiteClient client = getClient();
        if (client != null) {
            client.close();
        }
    }

    // this getter is intended explicitly for testing purposes
    protected SiteToSiteClient getClient() {
        return this.siteToSiteClient;
    }

    static class NiFiUrlValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final String value = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();

            URL url;
            try {
                url = new URL(value);
            } catch (final Exception e) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation("Not a valid URL")
                        .build();
            }

            if (url != null && !url.getPath().equals(DESTINATION_URL_PATH)) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation("URL path must be " + DESTINATION_URL_PATH)
                        .build();
            }

            return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(true)
                    .build();
        }
    }
}
