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
package org.apache.nifi.authorization;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileAuthorizerTest {

    private static final String EMPTY_AUTHORIZATIONS_CONCISE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<resources/>";

    private static final String EMPTY_AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<resources>"
        + "</resources>";

    private static final String BAD_SCHEMA_AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<resource>"
        + "</resource>";

    private static final String AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<resources>"
            + "<resource identifier=\"/flow\">"
                + "<authorization identity=\"user-1\" action=\"R\"/>"
            + "</resource>"
        + "</resources>";

    private static final String UPDATED_AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<resources>"
            + "<resource identifier=\"/flow\">"
                + "<authorization identity=\"user-1\" action=\"RW\"/>"
            + "</resource>"
        + "</resources>";

    private FileAuthorizer authorizer;
    private File primary;
    private File restore;

    private AuthorizerConfigurationContext configurationContext;

    @Before
    public void setup() throws IOException {
        // primary authorizations
        primary = new File("target/primary/authorizations.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(primary.getParentFile());

        // restore authorizations
        restore = new File("target/restore/authorizations.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(restore.getParentFile());

        final NiFiProperties properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restore.getParentFile());

        configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty(Mockito.eq("Authorizations File"))).thenReturn(new StandardPropertyValue(primary.getPath(), null));

        authorizer = new FileAuthorizer();
        authorizer.setNiFiProperties(properties);
        authorizer.initialize(null);
    }

    @After
    public void cleanup() throws Exception {
        deleteFile(primary);
        deleteFile(restore);
    }

    @Test
    public void testPostConstructionWhenRestoreDoesNotExist() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        assertEquals(primary.length(), restore.length());
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testPostConstructionWhenPrimaryDoesNotExist() throws Exception {
        writeAuthorizationsFile(restore, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testPostConstructionWhenPrimaryDifferentThanRestore() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS);
        writeAuthorizationsFile(restore, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testBadSchema() throws Exception {
        writeAuthorizationsFile(primary, BAD_SCHEMA_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
    }

    @Test
    public void testAuthorizedUserAction() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);

        final AuthorizationRequest request = new AuthorizationRequest.Builder().resource(ResourceFactory.getFlowResource()).identity("user-1").anonymous(false).accessAttempt(true).action(RequestAction
            .READ).build();
        final AuthorizationResult result = authorizer.authorize(request);
        assertTrue(Result.Approved.equals(result.getResult()));
    }

    @Test
    public void testUnauthorizedUser() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);

        final AuthorizationRequest request =
            new AuthorizationRequest.Builder().resource(ResourceFactory.getFlowResource()).identity("user-2").anonymous(false).accessAttempt(true).action(RequestAction.READ).build();
        final AuthorizationResult result = authorizer.authorize(request);
        assertFalse(Result.Approved.equals(result.getResult()));
    }

    @Test
    public void testUnauthorizedAction() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);

        final AuthorizationRequest request =
            new AuthorizationRequest.Builder().resource(ResourceFactory.getFlowResource()).identity("user-1").anonymous(false).accessAttempt(true).action(RequestAction.WRITE).build();
        final AuthorizationResult result = authorizer.authorize(request);
        assertFalse(Result.Approved.equals(result.getResult()));
    }

    @Test
    public void testReloadAuthorizations() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        when(configurationContext.getProperty(Mockito.eq("Reload Interval"))).thenReturn(new StandardPropertyValue("1 sec", null));
        authorizer.onConfigured(configurationContext);

        // ensure the user currently does not have write access
        final AuthorizationRequest request =
            new AuthorizationRequest.Builder().resource(ResourceFactory.getFlowResource()).identity("user-1").anonymous(false).accessAttempt(true).action(RequestAction.WRITE).build();
        AuthorizationResult result = authorizer.authorize(request);
        assertFalse(Result.Approved.equals(result.getResult()));

        // add write access for the user
        writeAuthorizationsFile(primary, UPDATED_AUTHORIZATIONS);

        // wait at least one second for the file to be stale
        Thread.sleep(4000L);

        // ensure the user does have write access now using the same request
        result = authorizer.authorize(request);
        assertTrue(Result.Approved.equals(result.getResult()));
    }

    private static void writeAuthorizationsFile(final File file, final String content) throws Exception {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(bytes);
        }
    }

    private static boolean deleteFile(final File file) {
        if (file.isDirectory()) {
            FileUtils.deleteFilesInDir(file, null, null, true, true);
        }
        return FileUtils.deleteFile(file, null, 10);
    }
}
