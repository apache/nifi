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
package org.apache.nifi.services.azure.keyvault;

import com.azure.security.keyvault.secrets.SecretClient;

import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

class MockConnectionService extends AzureKeyVaultClientService {

    private String MOCK_SECRET_NAME = "mockSecretName1";
    private String MOCK_SECRET_VALUE = "mockSecretValue1";
    private String MOCK_INVALID_SECRET_VALUE = "mockInvalidSecretValue1";

    private SecretClient getKeyVaultSecretClient(){
        return mock(SecretClient.class);
    }

    @Override
    public String getSecretFromKeyVault(String secretName) {
        if (secretName == MOCK_SECRET_NAME) {
            return MOCK_SECRET_VALUE;
        } else {
            return MOCK_INVALID_SECRET_VALUE;
        }
    }

}

public class TestAzureKeyVaultClientService {

    private TestRunner testRunner;
    private MockConnectionService kvService;

    private String MOCK_SECRET_NAME = "mockSecretName1";
    private String MOCK_SECRET_VALUE = "mockSecretValue1";
    private String MOCK_INVALID_SECRET_VALUE = "mockInvalidSecretValue1";

    @Before
    public void setup() throws Exception {
        kvService = new MockConnectionService();
        testRunner = TestRunners.newTestRunner(NoOpProcessor.class);
        testRunner.addControllerService("kvService", kvService);
    }

    @Test
    public void testGetKeyVaultSecret() {
        assertEquals(MOCK_SECRET_VALUE , kvService.getSecretFromKeyVault(MOCK_SECRET_NAME));
    }

    @Test
    public void testInvalidGetKeyVaultSecret() {
        assertNotEquals(MOCK_INVALID_SECRET_VALUE , kvService.getSecretFromKeyVault(MOCK_SECRET_NAME));
    }

    @Test
    public void testGetSecret() {
        assertEquals(MOCK_SECRET_VALUE, kvService.getSecret(MOCK_SECRET_NAME));
    }

    @Test
    public void testInvalidGetKeySecret() {
        assertNotEquals(MOCK_INVALID_SECRET_VALUE, kvService.getSecret(MOCK_SECRET_NAME));
    }
}

