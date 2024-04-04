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
package org.apache.nifi.processors.azure.storage.utils;

import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.services.azure.storage.ADLSCredentialsDetails;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

@ExtendWith(MockitoExtension.class)
class DataLakeServiceClientFactoryTest {

    @Mock
    private ComponentLog logger;

    @Test
    void testThatServiceClientIsCachedByCredentials() {
        final DataLakeServiceClientFactory clientFactory = new DataLakeServiceClientFactory(logger, null);

        final ADLSCredentialsDetails credentials = createCredentialDetails("account");

        final DataLakeServiceClient clientOne = clientFactory.getStorageClient(credentials);
        final DataLakeServiceClient clientTwo = clientFactory.getStorageClient(credentials);

        assertSame(clientOne, clientTwo);
    }

    @Test
    void testThatDifferentServiceClientIsReturnedForDifferentCredentials() {
        final DataLakeServiceClientFactory clientFactory = new DataLakeServiceClientFactory(logger, null);

        final ADLSCredentialsDetails credentialsOne = createCredentialDetails("accountOne");
        final ADLSCredentialsDetails credentialsTwo = createCredentialDetails("accountTwo");

        final DataLakeServiceClient clientOne = clientFactory.getStorageClient(credentialsOne);
        final DataLakeServiceClient clientTwo = clientFactory.getStorageClient(credentialsTwo);

        assertNotSame(clientOne, clientTwo);
    }

    @Test
    void testThatCachedClientIsReturnedAfterDifferentClientIsCreated() {
        final DataLakeServiceClientFactory clientFactory = new DataLakeServiceClientFactory(logger, null);

        final ADLSCredentialsDetails credentialsOne = createCredentialDetails("accountOne");
        final ADLSCredentialsDetails credentialsTwo = createCredentialDetails("accountTwo");
        final ADLSCredentialsDetails credentialsThree = createCredentialDetails("accountOne");

        final DataLakeServiceClient clientOne = clientFactory.getStorageClient(credentialsOne);
        final DataLakeServiceClient clientTwo = clientFactory.getStorageClient(credentialsTwo);
        final DataLakeServiceClient clientThree = clientFactory.getStorageClient(credentialsThree);

        assertNotSame(clientOne, clientTwo);
        assertSame(clientOne, clientThree);
    }

    private ADLSCredentialsDetails createCredentialDetails(String accountName) {
        return ADLSCredentialsDetails.Builder.newBuilder()
                .setAccountName(accountName)
                .setAccountKey("accountKey")
                .setEndpointSuffix("dfs.core.windows.net")
                .build();
    }
}