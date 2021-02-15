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
package org.apache.nifi.bootstrap.http;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.bootstrap.notification.NotificationContext;
import org.apache.nifi.bootstrap.notification.NotificationFailedException;
import org.apache.nifi.bootstrap.notification.NotificationInitializationContext;
import org.apache.nifi.bootstrap.notification.NotificationType;
import org.apache.nifi.bootstrap.notification.http.HttpNotificationService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_URL;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_CONNECTION_TIMEOUT;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_WRITE_TIMEOUT;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_KEYSTORE;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_KEYSTORE_PASSWORD;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_KEYSTORE_TYPE;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_KEY_PASSWORD;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_TRUSTSTORE;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_TRUSTSTORE_PASSWORD;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.PROP_TRUSTSTORE_TYPE;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.NOTIFICATION_SUBJECT_KEY;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.NOTIFICATION_TYPE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class HttpNotificationServiceTest {

    private static final long REQUEST_TIMEOUT = 2;

    private static final String SUBJECT = "Subject";

    private static final String MESSAGE = "Message";

    private static final String LOCALHOST = "localhost";

    private static final String BASE_PATH = "/";

    private static final String TIMEOUT = "10s";

    private MockWebServer mockWebServer;

    @Before
    public void startServer() {
        mockWebServer = new MockWebServer();
    }

    @After
    public void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testStartNotification() throws InterruptedException, NotificationFailedException {
        enqueueResponseCode(200);

        final Map<PropertyDescriptor, PropertyValue> properties = getProperties();
        final HttpNotificationService service = getHttpNotificationService(properties);
        service.notify(getNotificationContext(), NotificationType.NIFI_STARTED, SUBJECT, MESSAGE);

        assertRequestMatches(NotificationType.NIFI_STARTED);
    }

    @Test
    public void testStopNotification() throws InterruptedException, NotificationFailedException {
        enqueueResponseCode(200);

        final Map<PropertyDescriptor, PropertyValue> properties = getProperties();
        final HttpNotificationService service = getHttpNotificationService(properties);
        service.notify(getNotificationContext(), NotificationType.NIFI_STOPPED, SUBJECT, MESSAGE);

        assertRequestMatches(NotificationType.NIFI_STOPPED);
    }

    @Test
    public void testDiedNotification() throws InterruptedException, NotificationFailedException {
        enqueueResponseCode(200);

        final Map<PropertyDescriptor, PropertyValue> properties = getProperties();
        final HttpNotificationService service = getHttpNotificationService(properties);
        service.notify(getNotificationContext(), NotificationType.NIFI_DIED, SUBJECT, MESSAGE);

        assertRequestMatches(NotificationType.NIFI_DIED);
    }

    @Test
    public void testStartNotificationFailure() throws InterruptedException {
        enqueueResponseCode(500);

        final Map<PropertyDescriptor, PropertyValue> properties = getProperties();
        final HttpNotificationService service = getHttpNotificationService(properties);
        assertThrows(NotificationFailedException.class, () -> service.notify(getNotificationContext(), NotificationType.NIFI_STARTED, SUBJECT, MESSAGE));

        assertRequestMatches(NotificationType.NIFI_STARTED);
    }

    @Test
    public void testStartNotificationHttps() throws GeneralSecurityException, NotificationFailedException, InterruptedException, IOException {
        final TlsConfiguration tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();

        try {
            final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration);
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
            mockWebServer.useHttps(sslSocketFactory, false);

            enqueueResponseCode(200);

            final Map<PropertyDescriptor, PropertyValue> properties = getProperties();

            properties.put(PROP_KEYSTORE, createPropertyValue(tlsConfiguration.getKeystorePath()));
            properties.put(PROP_KEYSTORE_PASSWORD, createPropertyValue(tlsConfiguration.getKeystorePassword()));
            properties.put(PROP_KEY_PASSWORD, createPropertyValue(tlsConfiguration.getKeyPassword()));
            properties.put(PROP_KEYSTORE_TYPE, createPropertyValue(tlsConfiguration.getKeystoreType().getType()));
            properties.put(PROP_TRUSTSTORE, createPropertyValue(tlsConfiguration.getTruststorePath()));
            properties.put(PROP_TRUSTSTORE_PASSWORD, createPropertyValue(tlsConfiguration.getTruststorePassword()));
            properties.put(PROP_TRUSTSTORE_TYPE, createPropertyValue(tlsConfiguration.getTruststoreType().getType()));

            final HttpNotificationService service = getHttpNotificationService(properties);
            service.notify(getNotificationContext(), NotificationType.NIFI_STARTED, SUBJECT, MESSAGE);

            assertRequestMatches(NotificationType.NIFI_STARTED);
        } finally {
            Files.deleteIfExists(Paths.get(tlsConfiguration.getKeystorePath()));
            Files.deleteIfExists(Paths.get(tlsConfiguration.getTruststorePath()));
        }
    }

    private void assertRequestMatches(final NotificationType notificationType) throws InterruptedException {
        final RecordedRequest recordedRequest = mockWebServer.takeRequest(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(recordedRequest);
        assertEquals(notificationType.name(), recordedRequest.getHeader(NOTIFICATION_TYPE_KEY));
        assertEquals(SUBJECT, recordedRequest.getHeader(NOTIFICATION_SUBJECT_KEY));

        final Buffer bodyBuffer = recordedRequest.getBody();
        final String bodyString = new String(bodyBuffer.readByteArray(), UTF_8);
        assertEquals(MESSAGE, bodyString);
    }

    private void enqueueResponseCode(final int responseCode) {
        mockWebServer.enqueue(new MockResponse().setResponseCode(responseCode));
    }

    private HttpNotificationService getHttpNotificationService(final Map<PropertyDescriptor, PropertyValue> properties) {
        final HttpNotificationService service = new HttpNotificationService();
        final NotificationInitializationContext context = new NotificationInitializationContext() {
            @Override
            public String getIdentifier() {
                return NotificationInitializationContext.class.getName();
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                return properties.get(descriptor);
            }

            @Override
            public Map<String, String> getAllProperties() {
                final Map<String, String> allProperties = new HashMap<>();
                for (final Map.Entry<PropertyDescriptor, PropertyValue> entry : properties.entrySet()) {
                    allProperties.put(entry.getKey().getName(), entry.getValue().getValue());
                }
                return allProperties;
            }
        };

        service.initialize(context);
        return service;
    }

    private Map<PropertyDescriptor, PropertyValue> getProperties() {
        final Map<PropertyDescriptor, PropertyValue> properties = new HashMap<>();

        // Setting localhost is necessary to avoid hostname verification failures on Windows
        final String url = mockWebServer.url(BASE_PATH).newBuilder().host(LOCALHOST).build().toString();
        properties.put(PROP_URL, createPropertyValue(url));
        properties.put(PROP_CONNECTION_TIMEOUT, createPropertyValue(TIMEOUT));
        properties.put(PROP_WRITE_TIMEOUT, createPropertyValue(TIMEOUT));

        return properties;
    }

    private PropertyValue createPropertyValue(final String value) {
        return new StandardPropertyValue(value, null, null);
    }

    private NotificationContext getNotificationContext() {
        final Map<PropertyDescriptor, PropertyValue> properties = getProperties();
        return new NotificationContext() {
            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                return properties.get(descriptor);
            }

            @Override
            public Map<PropertyDescriptor, String> getProperties() {
                final Map<PropertyDescriptor, String> propertyValues = new HashMap<>();
                for (final Map.Entry<PropertyDescriptor, PropertyValue> entry : properties.entrySet()) {
                    propertyValues.put(entry.getKey(), entry.getValue().getValue());
                }
                return propertyValues;
            }
        };
    }
}
