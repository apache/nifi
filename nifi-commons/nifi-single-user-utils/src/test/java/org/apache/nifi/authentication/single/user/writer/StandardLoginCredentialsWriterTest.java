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
package org.apache.nifi.authentication.single.user.writer;

import org.apache.nifi.authentication.single.user.SingleUserCredentials;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class StandardLoginCredentialsWriterTest {
    private static final String BLANK_PROVIDERS = "/conf/login-identity-providers.xml";

    private static final String POPULATED_PROVIDERS = "/conf/populated-login-identity-providers.xml";

    private static final String XML_SUFFIX = ".xml";

    private static final String PROVIDER_CLASS = SingleUserCredentials.class.getName();

    @Test
    public void testWriteLoginCredentialsBlankProviders() throws IOException, URISyntaxException {
        final Path sourceProvidersPath = Paths.get(getClass().getResource(BLANK_PROVIDERS).toURI());
        assertCredentialsFound(sourceProvidersPath);
    }

    @Test
    public void testWriteLoginCredentialsPopulatedProviders() throws IOException, URISyntaxException {
        final Path sourceProvidersPath = Paths.get(getClass().getResource(POPULATED_PROVIDERS).toURI());
        assertCredentialsFound(sourceProvidersPath);
    }

    private void assertCredentialsFound(final Path sourceProvidersPath) throws IOException {
        final Path configuredProvidersPath = Files.createTempFile(getClass().getSimpleName(), XML_SUFFIX);
        configuredProvidersPath.toFile().deleteOnExit();

        Files.copy(sourceProvidersPath, configuredProvidersPath, StandardCopyOption.REPLACE_EXISTING);

        final StandardLoginCredentialsWriter writer = new StandardLoginCredentialsWriter(configuredProvidersPath.toFile());

        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        final SingleUserCredentials credentials = new SingleUserCredentials(username, password, PROVIDER_CLASS);
        writer.writeLoginCredentials(credentials);

        final String configuration = new String(Files.readAllBytes(configuredProvidersPath));
        assertTrue("Username not found", configuration.contains(username));
        assertTrue("Password not found", configuration.contains(password));
    }
}
