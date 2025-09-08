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

package org.apache.nifi.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceLookup;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MongoDBControllerServiceTest {

    private static final String IDENTIFIER = "mongodb-client";

    private TestRunner runner;
    private MongoDBControllerService service;

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new MongoDBControllerService();
        runner.addControllerService(IDENTIFIER, service);
    }

    @AfterEach
    public void tearDown() {
        try {
            if (service != null) {
                service.onDisable();
            }
        } catch (final Exception ignored) {
            // Ignore during cleanup
        }
    }

    private MongoClientSettings getClientSettings(final MongoDBControllerService svc) {
        final MongoClient client = svc.mongoClient; // protected field, accessible in-package
        assertNotNull(client, "MongoClient should have been initialized");
        final MongoClientImpl impl = (MongoClientImpl) client;
        return impl.getSettings();
    }

    private Map<PropertyDescriptor, String> getClientServiceProperties() {
        return ((MockControllerServiceLookup) runner.getProcessContext().getControllerServiceLookup())
                .getControllerServices().get(IDENTIFIER).getProperties();
    }

    @Test
    public void testAllInUri_ScramSha256_WithDatabase() throws Exception {
        final String uri = "mongodb://user1:pass1@localhost:27017/db1?authMechanism=SCRAM-SHA-256";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.enableControllerService(service);

        final MongoCredential credential = getClientSettings(service).getCredential();
        assertNotNull(credential, "Credential should be present from URI");
        assertEquals("user1", credential.getUserName());
        assertEquals("db1", credential.getSource());
    }

    @Test
    public void testAllInUri_X509_WithUserInUri() throws Exception {
        final String uri = "mongodb://CN=uriUser@localhost:27017/?authMechanism=MONGODB-X509";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.enableControllerService(service);

        final MongoCredential credential = getClientSettings(service).getCredential();
        assertNotNull(credential, "Credential should be present from URI");
        assertEquals("CN=uriUser", credential.getUserName());
        // X.509 credentials use $external authentication database
        assertEquals("$external", credential.getSource());
    }

    @Test
    public void testUriPlusUser_X509() throws Exception {
        final String uri = "mongodb://localhost:27017/?authMechanism=MONGODB-X509";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.setProperty(service, MongoDBControllerService.DB_USER, "CN=propertyUser");
        runner.enableControllerService(service);

        final MongoCredential credential = getClientSettings(service).getCredential();
        assertNotNull(credential, "Credential should be present from properties");
        assertEquals("CN=propertyUser", credential.getUserName());
        assertEquals("$external", credential.getSource());
    }

    @Test
    public void testUriPlusUser_ScramSha256_ShouldFailWithoutPassword() {
        final String uri = "mongodb://localhost:27017/db2?authMechanism=SCRAM-SHA-256";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.setProperty(service, MongoDBControllerService.DB_USER, "userOnly");

        // Build configuration context without enabling to capture exception from createClient
        final MockConfigurationContext context = new MockConfigurationContext(
                service,
                getClientServiceProperties(),
                runner.getProcessContext().getControllerServiceLookup(),
                null
        );

        assertThrows(IllegalArgumentException.class, () -> service.createClient(context, null));
    }

    @Test
    public void testUriPlusUserPassword_ScramSha256() throws Exception {
        final String uri = "mongodb://localhost:27017/db3?authMechanism=SCRAM-SHA-256";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.setProperty(service, MongoDBControllerService.DB_USER, "user256");
        runner.setProperty(service, MongoDBControllerService.DB_PASSWORD, "pass256");
        runner.enableControllerService(service);

        final MongoCredential credential = getClientSettings(service).getCredential();
        assertNotNull(credential);
        assertEquals("user256", credential.getUserName());
        assertEquals("db3", credential.getSource());
    }

    @Test
    public void testUriPlusUserPassword_Plain() throws Exception {
        final String uri = "mongodb://localhost:27017/mydb?authMechanism=PLAIN";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.setProperty(service, MongoDBControllerService.DB_USER, "plainUser");
        runner.setProperty(service, MongoDBControllerService.DB_PASSWORD, "plainPass");
        runner.enableControllerService(service);

        final MongoCredential credential = getClientSettings(service).getCredential();
        assertNotNull(credential);
        assertEquals("plainUser", credential.getUserName());
        assertEquals("mydb", credential.getSource());
    }

    @Test
    public void testUriPlusUserPassword_Aws() throws Exception {
        final String uri = "mongodb://localhost:27017/?authMechanism=MONGODB-AWS";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.setProperty(service, MongoDBControllerService.DB_USER, "awsUser");
        runner.setProperty(service, MongoDBControllerService.DB_PASSWORD, "awsSecret");
        runner.enableControllerService(service);

        final MongoCredential credential = getClientSettings(service).getCredential();
        assertNotNull(credential);
        assertEquals("awsUser", credential.getUserName());
        // AWS does not use a database in the same way; source is "$external"
        assertEquals("$external", credential.getSource());
    }

    @Test
    public void testUriPlusUserPassword_X509_ShouldFail() {
        final String uri = "mongodb://localhost:27017/?authMechanism=MONGODB-X509";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.setProperty(service, MongoDBControllerService.DB_USER, "CN=propUser");
        runner.setProperty(service, MongoDBControllerService.DB_PASSWORD, "ignored");

        final MockConfigurationContext context = new MockConfigurationContext(
                service,
                getClientServiceProperties(),
                runner.getProcessContext().getControllerServiceLookup(),
                null
        );

        assertThrows(IllegalArgumentException.class, () -> service.createClient(context, null));
    }

    @Test
    public void testX509_PropertyOverridesUserInUri() throws Exception {
        final String uri = "mongodb://CN=uriPreferred@localhost:27017/?authMechanism=MONGODB-X509";

        runner.setProperty(service, MongoDBControllerService.URI, uri);
        runner.setProperty(service, MongoDBControllerService.DB_USER, "CN=fromProperty");
        runner.enableControllerService(service);

        final MongoCredential credential = getClientSettings(service).getCredential();
        assertNotNull(credential);
        // Properties override URI when both provided
        assertEquals("CN=fromProperty", credential.getUserName());
        assertEquals("$external", credential.getSource());
    }
}
