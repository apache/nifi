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
package org.apache.nifi.bootstrap.notification.http;

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.bootstrap.notification.AbstractNotificationService;
import org.apache.nifi.bootstrap.notification.NotificationContext;
import org.apache.nifi.bootstrap.notification.NotificationFailedException;
import org.apache.nifi.bootstrap.notification.NotificationInitializationContext;
import org.apache.nifi.bootstrap.notification.NotificationType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class HttpNotificationService extends AbstractNotificationService {

    public static final String NOTIFICATION_TYPE_KEY = "notification.type";
    public static final String NOTIFICATION_SUBJECT_KEY = "notification.subject";

    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("URL")
            .description("The URL to send the notification to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection timeout")
            .description("Max wait time for connection to remote service.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    public static final PropertyDescriptor PROP_WRITE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Write timeout")
            .description("Max wait time for remote service to read the request sent.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();

    public static final PropertyDescriptor PROP_TRUSTSTORE = new PropertyDescriptor.Builder()
            .name("Truststore Filename")
            .description("The fully-qualified filename of the Truststore")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor PROP_TRUSTSTORE_TYPE = new PropertyDescriptor.Builder()
            .name("Truststore Type")
            .description("The Type of the Truststore")
            .allowableValues(KeystoreType.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor PROP_TRUSTSTORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("Truststore Password")
            .description("The password for the Truststore")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor PROP_KEYSTORE = new PropertyDescriptor.Builder()
            .name("Keystore Filename")
            .description("The fully-qualified filename of the Keystore")
            .defaultValue(null)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor PROP_KEYSTORE_TYPE = new PropertyDescriptor.Builder()
            .name("Keystore Type")
            .description("The Type of the Keystore")
            .allowableValues(KeystoreType.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor PROP_KEYSTORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("Keystore Password")
            .defaultValue(null)
            .description("The password for the Keystore")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor PROP_KEY_PASSWORD = new PropertyDescriptor.Builder()
            .name("Key Password")
            .displayName("Key Password")
            .description("The password for the key. If this is not specified, but the Keystore Filename, Password, and Type are specified, "
                    + "then the Keystore Password will be assumed to be the same as the Key Password.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor SSL_ALGORITHM = new PropertyDescriptor.Builder()
            .name("SSL Protocol")
            .defaultValue("TLS")
            .allowableValues("SSL", "TLS")
            .description("The algorithm to use for this SSL context.")
            .sensitive(false)
            .build();

    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();
    private final AtomicReference<String> urlReference = new AtomicReference<>();

    private static final List<PropertyDescriptor> supportedProperties;

    static {
        supportedProperties = new ArrayList<>();
        supportedProperties.add(PROP_URL);
        supportedProperties.add(PROP_CONNECTION_TIMEOUT);
        supportedProperties.add(PROP_WRITE_TIMEOUT);
        supportedProperties.add(PROP_TRUSTSTORE);
        supportedProperties.add(PROP_TRUSTSTORE_PASSWORD);
        supportedProperties.add(PROP_TRUSTSTORE_TYPE);
        supportedProperties.add(PROP_KEYSTORE);
        supportedProperties.add(PROP_KEYSTORE_PASSWORD);
        supportedProperties.add(PROP_KEYSTORE_TYPE);
        supportedProperties.add(PROP_KEY_PASSWORD);

    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedProperties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
    }

    @Override
    protected void init(final NotificationInitializationContext context) {
        final String url = context.getProperty(PROP_URL).evaluateAttributeExpressions().getValue();
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Property, \"" + PROP_URL.getDisplayName() + "\", for the URL to POST notifications to must be set.");
        }

        urlReference.set(url);

        httpClientReference.set(null);

        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

        Long connectTimeout = context.getProperty(PROP_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        Long writeTimeout = context.getProperty(PROP_WRITE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);

        // Set timeouts
        okHttpClientBuilder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
        okHttpClientBuilder.writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);

        // check if the keystore is set and add the factory if so
        if (url.toLowerCase().startsWith("https")) {
            try {
                final TlsConfiguration tlsConfiguration = createTlsConfigurationFromContext(context);
                final X509TrustManager x509TrustManager = SslContextFactory.getX509TrustManager(tlsConfiguration);
                if (x509TrustManager == null) {
                    throw new IllegalStateException("Unable to get X.509 Trust Manager for HTTP Notification Service configured for TLS");
                }

                final TrustManager[] trustManagers = new TrustManager[] { x509TrustManager };
                final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration, trustManagers);
                if (sslContext == null) {
                    throw new IllegalStateException("Unable to get SSL Context for HTTP Notification Service configured for TLS");
                }

                final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
                okHttpClientBuilder.sslSocketFactory(sslSocketFactory, x509TrustManager);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        httpClientReference.set(okHttpClientBuilder.build());
    }

    private static TlsConfiguration createTlsConfigurationFromContext(NotificationInitializationContext context) {
        String keystorePath = context.getProperty(HttpNotificationService.PROP_KEYSTORE).getValue();
        String keystorePassword = context.getProperty(HttpNotificationService.PROP_KEYSTORE_PASSWORD).getValue();
        String keyPassword = context.getProperty(HttpNotificationService.PROP_KEY_PASSWORD).getValue();
        String keystoreType = context.getProperty(HttpNotificationService.PROP_KEYSTORE_TYPE).getValue();
        String truststorePath = context.getProperty(HttpNotificationService.PROP_TRUSTSTORE).getValue();
        String truststorePassword = context.getProperty(HttpNotificationService.PROP_TRUSTSTORE_PASSWORD).getValue();
        String truststoreType = context.getProperty(HttpNotificationService.PROP_TRUSTSTORE_TYPE).getValue();
        return new StandardTlsConfiguration(keystorePath, keystorePassword, keyPassword, keystoreType, truststorePath, truststorePassword, truststoreType, TlsConfiguration.TLS_PROTOCOL);
    }

    @Override
    public void notify(NotificationContext context, NotificationType notificationType, String subject, String message) throws NotificationFailedException {
        try {
            final RequestBody requestBody = RequestBody.create(message, MediaType.parse("text/plain"));

            Request.Builder requestBuilder = new Request.Builder()
                    .post(requestBody)
                    .url(urlReference.get());

            Map<PropertyDescriptor, String> configuredProperties = context.getProperties();

            for (PropertyDescriptor propertyDescriptor : configuredProperties.keySet()) {
                if (propertyDescriptor.isDynamic()) {
                    String propertyValue = context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue();
                    requestBuilder = requestBuilder.addHeader(propertyDescriptor.getDisplayName(), propertyValue);
                }
            }

            final Request request = requestBuilder
                    .addHeader(NOTIFICATION_SUBJECT_KEY, subject)
                    .addHeader(NOTIFICATION_TYPE_KEY, notificationType.name())
                    .build();

            final OkHttpClient httpClient = httpClientReference.get();

            final Call call = httpClient.newCall(request);
            try (final Response response = call.execute()) {

                if (!response.isSuccessful()) {
                    throw new NotificationFailedException("Failed to send Http Notification. Received an unsuccessful status code response '" + response.code() + "'. The message was '" +
                            response.message() + "'");
                }
            }
        } catch (NotificationFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new NotificationFailedException("Failed to send Http Notification", e);
        }
    }
}
