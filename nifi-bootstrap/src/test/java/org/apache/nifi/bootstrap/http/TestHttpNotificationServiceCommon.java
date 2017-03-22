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
import org.apache.nifi.bootstrap.NotificationServiceManager;
import org.apache.nifi.bootstrap.notification.NotificationType;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.NOTIFICATION_SUBJECT_KEY;
import static org.apache.nifi.bootstrap.notification.http.HttpNotificationService.NOTIFICATION_TYPE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class TestHttpNotificationServiceCommon {

    public static String tempConfigFilePath;
    public static MockWebServer mockWebServer;

    @Test
    public void testStartNotification() throws ParserConfigurationException, SAXException, IOException, InterruptedException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        NotificationServiceManager notificationServiceManager = new NotificationServiceManager();
        notificationServiceManager.setMaxNotificationAttempts(1);
        notificationServiceManager.loadNotificationServices(new File(tempConfigFilePath));
        notificationServiceManager.registerNotificationService(NotificationType.NIFI_STARTED, "http-notification");
        notificationServiceManager.notify(NotificationType.NIFI_STARTED, "Subject", "Message");

        RecordedRequest recordedRequest = mockWebServer.takeRequest(2, TimeUnit.SECONDS);
        assertNotNull(recordedRequest);
        assertEquals(NotificationType.NIFI_STARTED.name(), recordedRequest.getHeader(NOTIFICATION_TYPE_KEY));
        assertEquals("Subject", recordedRequest.getHeader(NOTIFICATION_SUBJECT_KEY));
        assertEquals("testing", recordedRequest.getHeader("testProp"));

        Buffer bodyBuffer = recordedRequest.getBody();
        String bodyString =new String(bodyBuffer.readByteArray(), UTF_8);
        assertEquals("Message", bodyString);
    }

    @Test
    public void testStopNotification() throws ParserConfigurationException, SAXException, IOException, InterruptedException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        NotificationServiceManager notificationServiceManager = new NotificationServiceManager();
        notificationServiceManager.setMaxNotificationAttempts(1);
        notificationServiceManager.loadNotificationServices(new File(tempConfigFilePath));
        notificationServiceManager.registerNotificationService(NotificationType.NIFI_STOPPED, "http-notification");
        notificationServiceManager.notify(NotificationType.NIFI_STOPPED, "Subject", "Message");

        RecordedRequest recordedRequest = mockWebServer.takeRequest(2, TimeUnit.SECONDS);
        assertNotNull(recordedRequest);
        assertEquals(NotificationType.NIFI_STOPPED.name(), recordedRequest.getHeader(NOTIFICATION_TYPE_KEY));
        assertEquals("Subject", recordedRequest.getHeader(NOTIFICATION_SUBJECT_KEY));

        Buffer bodyBuffer = recordedRequest.getBody();
        String bodyString =new String(bodyBuffer.readByteArray(), UTF_8);
        assertEquals("Message", bodyString);
    }

    @Test
    public void testDiedNotification() throws ParserConfigurationException, SAXException, IOException, InterruptedException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        NotificationServiceManager notificationServiceManager = new NotificationServiceManager();
        notificationServiceManager.setMaxNotificationAttempts(1);
        notificationServiceManager.loadNotificationServices(new File(tempConfigFilePath));
        notificationServiceManager.registerNotificationService(NotificationType.NIFI_DIED, "http-notification");
        notificationServiceManager.notify(NotificationType.NIFI_DIED, "Subject", "Message");

        RecordedRequest recordedRequest = mockWebServer.takeRequest(2, TimeUnit.SECONDS);
        assertNotNull(recordedRequest);

        assertEquals(NotificationType.NIFI_DIED.name(), recordedRequest.getHeader(NOTIFICATION_TYPE_KEY));
        assertEquals("Subject", recordedRequest.getHeader(NOTIFICATION_SUBJECT_KEY));

        Buffer bodyBuffer = recordedRequest.getBody();
        String bodyString =new String(bodyBuffer.readByteArray(), UTF_8);
        assertEquals("Message", bodyString);
    }

    @Test
    public void testStartNotificationFailure() throws ParserConfigurationException, SAXException, IOException, InterruptedException {
        // Web server will still get the request but will return an error. Observe that it is gracefully handled.

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        NotificationServiceManager notificationServiceManager = new NotificationServiceManager();
        notificationServiceManager.setMaxNotificationAttempts(1);
        notificationServiceManager.loadNotificationServices(new File(tempConfigFilePath));
        notificationServiceManager.registerNotificationService(NotificationType.NIFI_STARTED, "http-notification");
        notificationServiceManager.notify(NotificationType.NIFI_STARTED, "Subject", "Message");

        RecordedRequest recordedRequest = mockWebServer.takeRequest(2, TimeUnit.SECONDS);
        assertNotNull(recordedRequest);
        assertEquals(NotificationType.NIFI_STARTED.name(), recordedRequest.getHeader(NOTIFICATION_TYPE_KEY));
        assertEquals("Subject", recordedRequest.getHeader(NOTIFICATION_SUBJECT_KEY));

        Buffer bodyBuffer = recordedRequest.getBody();
        String bodyString =new String(bodyBuffer.readByteArray(), UTF_8);
        assertEquals("Message", bodyString);
    }

}
