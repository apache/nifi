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
package org.apache.nifi.reporting.s2s;

import java.net.Proxy;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.ssl.SSLContextProvider;

public class SiteToSiteUtils {

    // Obsolete property names
    public static final String OBSOLETE_PROXY_HOST = "s2s-http-proxy-hostname";
    public static final String OBSOLETE_PROXY_PORT = "s2s-http-proxy-port";
    public static final String OBSOLETE_PROXY_USERNAME = "s2s-http-proxy-username";
    public static final String OBSOLETE_PROXY_PASSWORD = "s2s-http-proxy-password";

    public static final PropertyDescriptor DESTINATION_URL = new PropertyDescriptor.Builder()
            .name("Destination URL")
            .displayName("Destination URL")
            .description("The URL of the destination NiFi instance or, if clustered, a comma-separated list of address in the format "
                    + "of http(s)://host:port/nifi. This destination URL will only be used to initiate the Site-to-Site connection. The "
                    + "data sent by this reporting task will be load-balanced on all the nodes of the destination (if clustered).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(new NiFiUrlValidator())
            .build();
    public static final PropertyDescriptor PORT_NAME = new PropertyDescriptor.Builder()
            .name("Input Port Name")
            .displayName("Input Port Name")
            .description("The name of the Input Port to deliver data to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service to use when communicating with the destination. If not specified, communications will not be secure.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();
    public static final PropertyDescriptor INSTANCE_URL = new PropertyDescriptor.Builder()
            .name("Instance URL")
            .displayName("Instance URL")
            .description("The URL of this instance to use in the Content URI of each event.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("http://${hostname(true)}:8080/nifi")
            .addValidator(new NiFiUrlValidator())
            .build();
    public static final PropertyDescriptor COMPRESS = new PropertyDescriptor.Builder()
            .name("Compress Events")
            .displayName("Compress Events")
            .description("Indicates whether or not to compress the data being sent.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .displayName("Communications Timeout")
            .description("Specifies how long to wait to a response from the destination before deciding that an error has occurred and canceling the transaction")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("Specifies how many records to send in a single batch, at most.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor TRANSPORT_PROTOCOL = new PropertyDescriptor.Builder()
            .name("s2s-transport-protocol")
            .displayName("Transport Protocol")
            .description("Specifies which transport protocol to use for Site-to-Site communication.")
            .required(true)
            .allowableValues(SiteToSiteTransportProtocol.values())
            .defaultValue(SiteToSiteTransportProtocol.RAW.name())
            .build();
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP, ProxySpec.HTTP_AUTH))
            .dependsOn(TRANSPORT_PROTOCOL, SiteToSiteTransportProtocol.HTTP.name())
            .build();
    public static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder()
            .name("Platform")
            .description("The value to use for the platform field in each event.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static SiteToSiteClient getClient(PropertyContext reportContext, ComponentLog logger, StateManager stateManager) {
        final SSLContextProvider sslContextProvider = reportContext.getProperty(SiteToSiteUtils.SSL_CONTEXT).asControllerService(SSLContextProvider.class);
        final SSLContext sslContext = sslContextProvider == null ? null : sslContextProvider.createContext();
        final EventReporter eventReporter = (EventReporter) (severity, category, message) -> {
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
        };
        final String destinationUrl = reportContext.getProperty(SiteToSiteUtils.DESTINATION_URL).evaluateAttributeExpressions().getValue();

        final SiteToSiteTransportProtocol mode = SiteToSiteTransportProtocol.valueOf(reportContext.getProperty(SiteToSiteUtils.TRANSPORT_PROTOCOL).getValue());

        HttpProxy httpProxy = null;
        if (mode == SiteToSiteTransportProtocol.HTTP) {
            final ProxyConfigurationService proxyConfigurationService = reportContext.getProperty(SiteToSiteUtils.PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
            if (proxyConfigurationService != null) {
                final ProxyConfiguration proxyConfiguration = proxyConfigurationService.getConfiguration();
                if (proxyConfiguration.getProxyType() == Proxy.Type.HTTP) {
                    httpProxy = new HttpProxy(proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort(),
                            proxyConfiguration.getProxyUserName(), proxyConfiguration.getProxyUserPassword());
                }
            }
        }

        // If no state manager was provided and this context supports retrieving it, do so
        if (stateManager == null && reportContext instanceof ReportingContext) {
            stateManager = ((ReportingContext) reportContext).getStateManager();
        }
        return new SiteToSiteClient.Builder()
                .urls(SiteToSiteRestApiClient.parseClusterUrls(destinationUrl))
                .portName(reportContext.getProperty(SiteToSiteUtils.PORT_NAME).getValue())
                .useCompression(reportContext.getProperty(SiteToSiteUtils.COMPRESS).asBoolean())
                .eventReporter(eventReporter)
                .sslContext(sslContext)
                .timeout(reportContext.getProperty(SiteToSiteUtils.TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .transportProtocol(mode)
                .httpProxy(httpProxy)
                .stateManager(stateManager)
                .build();
    }

    public static class NiFiUrlValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final String value = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();
            try {
                SiteToSiteRestApiClient.parseClusterUrls(value);
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(true)
                        .build();
            } catch (IllegalArgumentException ex) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation(ex.getLocalizedMessage())
                        .build();
            }
        }
    }
}
