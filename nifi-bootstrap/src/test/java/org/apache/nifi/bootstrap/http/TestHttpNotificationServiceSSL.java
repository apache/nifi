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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.bootstrap.NotificationServiceManager;
import org.apache.nifi.security.util.SslContextFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.io.IOUtil;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import ch.qos.logback.classic.Logger;

import javax.net.ssl.SSLContext;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestHttpNotificationServiceSSL extends TestHttpNotificationServiceCommon {

    static final String CONFIGURATION_FILE_TEXT = "\n"+
            "<services>\n"+
            "         <service>\n"+
            "            <id>http-notification</id>\n"+
            "            <class>org.apache.nifi.bootstrap.notification.http.HttpNotificationService</class>\n"+
            "            <property name=\"URL\">${test.server}</property>\n"+
            "            <property name=\"Truststore Filename\">./src/test/resources/truststore.jks</property>\n"+
            "            <property name=\"Truststore Type\">JKS</property>\n"+
            "            <property name=\"Truststore Password\">passwordpassword</property>\n"+
            "            <property name=\"Keystore Filename\">./src/test/resources/keystore.jks</property>\n"+
            "            <property name=\"Keystore Type\">JKS</property>\n"+
            "            <property name=\"Key Password\">passwordpassword</property>\n"+
            "            <property name=\"Keystore Password\">passwordpassword</property>\n"+
            "            <property name=\"testProp\">${literal('testing')}</property>\n"+
            "         </service>\n"+
            "</services>";

    static final String CONFIGURATION_FILE_TEXT_NO_KEYSTORE_PASSWORD = "\n"+
            "<services>\n"+
            "         <service>\n"+
            "            <id>http-notification</id>\n"+
            "            <class>org.apache.nifi.bootstrap.notification.http.HttpNotificationService</class>\n"+
            "            <property name=\"URL\">${test.server}</property>\n"+
            "            <property name=\"Truststore Filename\">./src/test/resources/truststore.jks</property>\n"+
            "            <property name=\"Truststore Type\">JKS</property>\n"+
            "            <property name=\"Truststore Password\">passwordpassword</property>\n"+
            "            <property name=\"Keystore Filename\">./src/test/resources/keystore.jks</property>\n"+
            "            <property name=\"Keystore Type\">JKS</property>\n"+
            "            <property name=\"Key Password\">passwordpassword</property>\n"+
            "            <property name=\"testProp\">${literal('testing')}</property>\n"+
            "         </service>\n"+
            "</services>";

    static final String CONFIGURATION_FILE_TEXT_NO_KEY_PASSWORD = "\n"+
            "<services>\n"+
            "         <service>\n"+
            "            <id>http-notification</id>\n"+
            "            <class>org.apache.nifi.bootstrap.notification.http.HttpNotificationService</class>\n"+
            "            <property name=\"URL\">${test.server}</property>\n"+
            "            <property name=\"Truststore Filename\">./src/test/resources/truststore.jks</property>\n"+
            "            <property name=\"Truststore Type\">JKS</property>\n"+
            "            <property name=\"Truststore Password\">passwordpassword</property>\n"+
            "            <property name=\"Keystore Filename\">./src/test/resources/keystore.jks</property>\n"+
            "            <property name=\"Keystore Type\">JKS</property>\n"+
            "            <property name=\"Keystore Password\">passwordpassword</property>\n"+
            "            <property name=\"testProp\">${literal('testing')}</property>\n"+
            "         </service>\n"+
            "</services>";

    static final String CONFIGURATION_FILE_TEXT_BLANK_KEY_PASSWORD = "\n"+
            "<services>\n"+
            "         <service>\n"+
            "            <id>http-notification</id>\n"+
            "            <class>org.apache.nifi.bootstrap.notification.http.HttpNotificationService</class>\n"+
            "            <property name=\"URL\">${test.server}</property>\n"+
            "            <property name=\"Truststore Filename\">./src/test/resources/truststore.jks</property>\n"+
            "            <property name=\"Truststore Type\">JKS</property>\n"+
            "            <property name=\"Truststore Password\">passwordpassword</property>\n"+
            "            <property name=\"Keystore Filename\">./src/test/resources/keystore.jks</property>\n"+
            "            <property name=\"Keystore Type\">JKS</property>\n"+
            "            <property name=\"Keystore Password\">passwordpassword</property>\n"+
            "            <property name=\"Key Password\"></property>\n"+
            "            <property name=\"testProp\">${literal('testing')}</property>\n"+
            "         </service>\n"+
            "</services>";

    static final String CONFIGURATION_FILE_TEXT_BLANK_KEYSTORE_PASSWORD = "\n"+
            "<services>\n"+
            "         <service>\n"+
            "            <id>http-notification</id>\n"+
            "            <class>org.apache.nifi.bootstrap.notification.http.HttpNotificationService</class>\n"+
            "            <property name=\"URL\">${test.server}</property>\n"+
            "            <property name=\"Truststore Filename\">./src/test/resources/truststore.jks</property>\n"+
            "            <property name=\"Truststore Type\">JKS</property>\n"+
            "            <property name=\"Truststore Password\">passwordpassword</property>\n"+
            "            <property name=\"Keystore Filename\">./src/test/resources/keystore.jks</property>\n"+
            "            <property name=\"Keystore Type\">JKS</property>\n"+
            "            <property name=\"Keystore Password\"></property>\n"+
            "            <property name=\"Key Password\">passwordpassword</property>\n"+
            "            <property name=\"testProp\">${literal('testing')}</property>\n"+
            "         </service>\n"+
            "</services>";

    @Before
    public void startServer() throws IOException, UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        tempConfigFilePath = "./target/TestHttpNotificationService-config.xml";

        Files.deleteIfExists(Paths.get(tempConfigFilePath));

        mockWebServer = new MockWebServer();

        final SSLContext sslContext = SslContextFactory.createSslContext(
                "./src/test/resources/keystore.jks",
                "passwordpassword".toCharArray(),
                null,
                "JKS",
                "./src/test/resources/truststore.jks",
                "passwordpassword".toCharArray(),
                "JKS",
                SslContextFactory.ClientAuth.REQUIRED,
                "TLS");

        mockWebServer.useHttps(sslContext.getSocketFactory(), false);

        String configFileOutput = CONFIGURATION_FILE_TEXT.replace("${test.server}", String.valueOf(mockWebServer.url("/")));
        IOUtil.writeText(configFileOutput, new File(tempConfigFilePath));
    }

    @After
    public void shutdownServer() throws IOException {
        Files.deleteIfExists(Paths.get(tempConfigFilePath));
        mockWebServer.shutdown();
    }

    @Test
    public void testStartNotificationSucceedsNoKeystorePasswd() throws ParserConfigurationException, SAXException, IOException {
        Logger notificationServiceLogger = (Logger) LoggerFactory.getLogger(NotificationServiceManager.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        notificationServiceLogger.addAppender(listAppender);

        String configFileOutput = CONFIGURATION_FILE_TEXT_NO_KEYSTORE_PASSWORD.replace("${test.server}", String.valueOf(mockWebServer.url("/")));
        IOUtil.writeText(configFileOutput, new File(tempConfigFilePath));

        NotificationServiceManager notificationServiceManager = new NotificationServiceManager();
        notificationServiceManager.setMaxNotificationAttempts(1);
        notificationServiceManager.loadNotificationServices(new File(tempConfigFilePath));

        List<ILoggingEvent> logsList = listAppender.list;
        boolean notificationServiceFailed = false;
        for(ILoggingEvent logMessage : logsList) {
            if(logMessage.getFormattedMessage().contains("is not valid for the following reasons")) {
                    notificationServiceFailed = true;
            }
        }

        assertFalse(notificationServiceFailed);
    }

    @Test
    public void testStartNotificationSucceedsNoKeyPasswd() throws ParserConfigurationException, SAXException, IOException {
        Logger notificationServiceLogger = (Logger) LoggerFactory.getLogger(NotificationServiceManager.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        notificationServiceLogger.addAppender(listAppender);

        String configFileOutput = CONFIGURATION_FILE_TEXT_NO_KEY_PASSWORD.replace("${test.server}", String.valueOf(mockWebServer.url("/")));
        IOUtil.writeText(configFileOutput, new File(tempConfigFilePath));

        NotificationServiceManager notificationServiceManager = new NotificationServiceManager();
        notificationServiceManager.setMaxNotificationAttempts(1);
        notificationServiceManager.loadNotificationServices(new File(tempConfigFilePath));

        List<ILoggingEvent> logsList = listAppender.list;
        boolean notificationServiceFailed = false;
        for(ILoggingEvent logMessage : logsList) {
            if(logMessage.getFormattedMessage().contains("is not valid for the following reasons")) {
                notificationServiceFailed = true;
            }
        }

        assertFalse(notificationServiceFailed);
    }

    @Test
    public void testStartNotificationFailsBlankKeystorePasswdCorrectKeypasswd() throws ParserConfigurationException, SAXException, IOException {
        Logger notificationServiceLogger = (Logger) LoggerFactory.getLogger(NotificationServiceManager.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        notificationServiceLogger.addAppender(listAppender);

        String configFileOutput = CONFIGURATION_FILE_TEXT_BLANK_KEYSTORE_PASSWORD.replace("${test.server}", String.valueOf(mockWebServer.url("/")));
        IOUtil.writeText(configFileOutput, new File(tempConfigFilePath));

        NotificationServiceManager notificationServiceManager = new NotificationServiceManager();
        notificationServiceManager.setMaxNotificationAttempts(1);
        notificationServiceManager.loadNotificationServices(new File(tempConfigFilePath));

        List<ILoggingEvent> logsList = listAppender.list;
        boolean notificationServiceFailed = false;
        for(ILoggingEvent logMessage : logsList) {
            if(logMessage.getFormattedMessage().contains("'Keystore Password' validated against '' is invalid because Keystore Password cannot be empty")) {
                notificationServiceFailed = true;
            }
        }

        assertTrue(notificationServiceFailed);
    }

    @Test
    public void testStartNotificationFailsCorrectKeystorePasswdBlankKeypasswd() throws ParserConfigurationException, SAXException, IOException {
        Logger notificationServiceLogger = (Logger) LoggerFactory.getLogger(NotificationServiceManager.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        notificationServiceLogger.addAppender(listAppender);

        String configFileOutput = CONFIGURATION_FILE_TEXT_BLANK_KEY_PASSWORD.replace("${test.server}", String.valueOf(mockWebServer.url("/")));
        IOUtil.writeText(configFileOutput, new File(tempConfigFilePath));

        NotificationServiceManager notificationServiceManager = new NotificationServiceManager();
        notificationServiceManager.setMaxNotificationAttempts(1);
        notificationServiceManager.loadNotificationServices(new File(tempConfigFilePath));

        List<ILoggingEvent> logsList = listAppender.list;
        boolean notificationServiceFailed = false;
        for(ILoggingEvent logMessage : logsList) {
            if(logMessage.getFormattedMessage().contains("'Key Password' validated against '' is invalid because Key Password cannot be empty")) {
                notificationServiceFailed = true;
            }
        }

        assertTrue(notificationServiceFailed);
    }
}
