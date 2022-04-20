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
package org.apache.nifi.processors.azure.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.proxy.StandardProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsControllerService;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.BeforeEach;

import java.io.FileInputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public abstract class AbstractAzureStorageIT {

    private static final Properties CONFIG;
    private static final Properties PROXY_CONFIG;

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/azure-credentials.PROPERTIES";
    private static final String PROXY_CONFIGURATION_FILE = System.getProperty("user.home") + "/proxy-configuration.PROPERTIES";

    static {
        CONFIG = new Properties();
        PROXY_CONFIG = new Properties();
        assertDoesNotThrow(() -> {
            final FileInputStream cFis = new FileInputStream(CREDENTIALS_FILE);
            assertDoesNotThrow(() -> CONFIG.load(cFis));
            FileUtils.closeQuietly(cFis);

            final FileInputStream pCFis = new FileInputStream(PROXY_CONFIGURATION_FILE);
            assertDoesNotThrow(() -> PROXY_CONFIG.load(pCFis));
            FileUtils.closeQuietly(pCFis);
        });
    }

    protected String getAccountName() {
        return CONFIG.getProperty("accountName");
    }

    protected String getAccountKey() {
        return CONFIG.getProperty("accountKey");
    }

    protected String getEndpointSuffix() {
        String endpointSuffix = CONFIG.getProperty("endpointSuffix");
        return endpointSuffix != null ? endpointSuffix : getDefaultEndpointSuffix();
    }

    protected String getProxyType() {
        return PROXY_CONFIG.getProperty("proxyType");
    }

    protected String getSocksVersion() {
        return PROXY_CONFIG.getProperty("socksVersion");
    }

    protected String getProxyServerHost() {
        return PROXY_CONFIG.getProperty("proxyServerHost");
    }

    protected String getProxyServerPort() {
        return PROXY_CONFIG.getProperty("proxyServerPort");
    }

    protected String getProxyUsername() {
        return PROXY_CONFIG.getProperty("proxyUsername");
    }

    protected String getProxyUserPassword() {
        return PROXY_CONFIG.getProperty("proxyUserPassword");
    }

    protected abstract String getDefaultEndpointSuffix();

    protected TestRunner runner;

    @BeforeEach
    public void setUpAzureStorageIT() throws Exception {
        runner = TestRunners.newTestRunner(getProcessorClass());

        setUpCredentials();
    }

    protected void setUpCredentials() throws Exception {
        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, getAccountKey());
    }

    protected abstract Class<? extends Processor> getProcessorClass();

    protected CloudStorageAccount getStorageAccount() throws Exception {
        StorageCredentials storageCredentials = new StorageCredentialsAccountAndKey(getAccountName(), getAccountKey());
        return new CloudStorageAccount(storageCredentials, true);
    }

    protected void configureCredentialsService() throws Exception {
        runner.removeProperty(AzureStorageUtils.ACCOUNT_NAME);
        runner.removeProperty(AzureStorageUtils.ACCOUNT_KEY);

        AzureStorageCredentialsService credentialsService = new AzureStorageCredentialsControllerService();

        runner.addControllerService("credentials-service", credentialsService);

        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_NAME, getAccountName());
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_KEY, getAccountKey());

        runner.assertValid(credentialsService);

        runner.enableControllerService(credentialsService);

        runner.setProperty(AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE, credentialsService.getIdentifier());
    }

    protected void configureProxyService() throws InitializationException {
        final StandardProxyConfigurationService proxyConfigurationService = new StandardProxyConfigurationService();
        runner.addControllerService("proxy-configuration-service", proxyConfigurationService);

        runner.setProperty(proxyConfigurationService, StandardProxyConfigurationService.PROXY_TYPE, getProxyType());
        runner.setProperty(proxyConfigurationService, StandardProxyConfigurationService.SOCKS_VERSION, getSocksVersion());
        runner.setProperty(proxyConfigurationService, StandardProxyConfigurationService.PROXY_SERVER_HOST, getProxyServerHost());
        runner.setProperty(proxyConfigurationService, StandardProxyConfigurationService.PROXY_SERVER_PORT, getProxyServerPort());
        runner.setProperty(proxyConfigurationService, StandardProxyConfigurationService.PROXY_USER_NAME, getProxyUsername());
        runner.setProperty(proxyConfigurationService, StandardProxyConfigurationService.PROXY_USER_PASSWORD, getProxyUserPassword());

        runner.assertValid(proxyConfigurationService);

        runner.enableControllerService(proxyConfigurationService);

        runner.setProperty(AzureStorageUtils.PROXY_CONFIGURATION_SERVICE, proxyConfigurationService.getIdentifier());
    }
}
