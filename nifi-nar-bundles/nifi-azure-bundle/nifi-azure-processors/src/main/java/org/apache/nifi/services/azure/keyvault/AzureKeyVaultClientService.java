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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.StringUtils;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"azure", "keyvault", "credential", "service", "secure"})
@CapabilityDescription(
        "Provides a controller service that configures a connection to Azure Key Vault" +
                " and provides access to that connection to Azure Key Vault components."
)
public class AzureKeyVaultClientService
        extends AbstractControllerService
        implements AzureKeyVaultConnectionService {

    private String keyVaultName;
    private String servicePrincipalClientID;
    private String servicePrincipalClientSecret;
    private String tenantID;
    private String endPointSuffix;
    private Boolean useManagedIdentity;
    private SecretClient keyVaultSecretClient;
    private ComponentLog logger;
    private LoadingCache<String, String> secretCache;

    public String getKeyVaultName() {
        return this.keyVaultName;
    }

    public String getServicePrincipalClientID() {
        return this.servicePrincipalClientID;
    }

    public String getServicePrincipalClientSecret() {
        return this.servicePrincipalClientSecret;
    }

    public String getTenantID() {
        return this.tenantID;
    }

    public String getEndPointSuffix() {
        return this.endPointSuffix;
    }

    public Boolean getUseManagedIdentity() {
        return this.useManagedIdentity;
    }

    @Override
    public SecretClient getKeyVaultSecretClient() {
        return this.keyVaultSecretClient;
    }

    @Override
    public String getSecretFromKeyVault(String secretName) {
        return this.keyVaultSecretClient.getSecret(secretName).getValue();
    }

    @Override
    public String getSecret(String secretName) {
        if (secretCache != null) {
            try {
                return secretCache.get(secretName);
            } catch (final Exception e) {
                logger.error("Failed to get secret '"+ secretName +"' from cache", e);
            }
        }
        return getSecretFromKeyVault(secretName);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        logger = getLogger();
        this.keyVaultName = context.getProperty(
                AzureKeyVaultUtils.KEYVAULT_NAME).getValue();
        this.servicePrincipalClientID = context.getProperty(
                AzureKeyVaultUtils.SP_CLIENT_ID).getValue();
        this.servicePrincipalClientSecret = context.getProperty(
                AzureKeyVaultUtils.SP_CLIENT_SECRET).getValue();
        this.tenantID = context.getProperty(
                AzureKeyVaultUtils.TENANT_ID).getValue();
        this.endPointSuffix = context.getProperty(
                AzureKeyVaultUtils.ENDPOINT_SUFFIX).getValue();
        this.useManagedIdentity = context.getProperty(
                AzureKeyVaultUtils.USE_MANAGED_IDENTITY).asBoolean();

        createKeyVaultSecretClient();

        final Integer cacheSize = context.getProperty(AzureKeyVaultUtils.CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(
                AzureKeyVaultUtils.CACHE_TTL_AFTER_WRITE
        ).asTimePeriod(TimeUnit.SECONDS);

        if (cacheSize > 0) {
            CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheTTL > 0) {
                cacheBuilder = cacheBuilder.expireAfterWrite(cacheTTL, TimeUnit.SECONDS);
            }

            logger.info(String.format(
                    "Secret cache enabled with cacheSize: %d and cacheTTL: %d secs",
                    cacheSize, cacheTTL));
            secretCache = cacheBuilder.build(
                    new CacheLoader<String, String>() {
                        @Override
                        public String load(String secretName) throws Exception {
                            return getSecretFromKeyVault(secretName);
                        }
                    });
        } else {
            secretCache = null;
            logger.info("Secret cache disabled because cache size is set to 0");
        }
    }

    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(AzureKeyVaultUtils.KEYVAULT_NAME);
        descriptors.add(AzureKeyVaultUtils.SP_CLIENT_ID);
        descriptors.add(AzureKeyVaultUtils.SP_CLIENT_SECRET);
        descriptors.add(AzureKeyVaultUtils.TENANT_ID);
        descriptors.add(AzureKeyVaultUtils.ENDPOINT_SUFFIX);
        descriptors.add(AzureKeyVaultUtils.USE_MANAGED_IDENTITY);
        descriptors.add(AzureKeyVaultUtils.CACHE_SIZE);
        descriptors.add(AzureKeyVaultUtils.CACHE_TTL_AFTER_WRITE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String keyVaultName = validationContext.getProperty(
                AzureKeyVaultUtils.KEYVAULT_NAME).getValue();
        final String clientID = validationContext.getProperty(
                AzureKeyVaultUtils.SP_CLIENT_ID).getValue();
        final String clientSecret = validationContext.getProperty(
                AzureKeyVaultUtils.SP_CLIENT_SECRET).getValue();
        final String tenantID = validationContext.getProperty(
                AzureKeyVaultUtils.TENANT_ID).getValue();
        final Boolean useManagedIdentity = validationContext.getProperty(
                AzureKeyVaultUtils.USE_MANAGED_IDENTITY).asBoolean();

        if (StringUtils.isBlank(keyVaultName)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation(AzureKeyVaultUtils.KEYVAULT_NAME.getDisplayName() +" is required")
                    .build());
        } else if (useManagedIdentity && (StringUtils.isNotBlank(clientID)
                || StringUtils.isNotBlank(clientSecret))) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation("if " + AzureKeyVaultUtils.USE_MANAGED_IDENTITY.getDisplayName()
                            + " is used then " + AzureKeyVaultUtils.SP_CLIENT_ID.getDisplayName()
                            + ", " + AzureKeyVaultUtils.SP_CLIENT_SECRET.getDisplayName()
                            + ", " + AzureKeyVaultUtils.TENANT_ID.getDisplayName()
                            + " should be blank.")
                    .build());
        } else if (!useManagedIdentity && (StringUtils.isBlank(clientID)
                || StringUtils.isBlank(clientSecret)
                || StringUtils.isBlank(tenantID))) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation("all of " + AzureKeyVaultUtils.SP_CLIENT_ID.getDisplayName()
                            + " and " + AzureKeyVaultUtils.SP_CLIENT_SECRET.getDisplayName()
                            + " and " + AzureKeyVaultUtils.TENANT_ID.getDisplayName() +" are required")
                    .build());
        }
        return results;
    }

    protected void createKeyVaultSecretClient(){
        String kvUri = "https://" + this.keyVaultName + this.endPointSuffix;

        if (this.useManagedIdentity) {
            this.keyVaultSecretClient = new SecretClientBuilder()
                    .vaultUrl(kvUri)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();
        } else {
            ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                    .clientId(this.servicePrincipalClientID)
                    .clientSecret(this.servicePrincipalClientSecret)
                    .tenantId(this.tenantID)
                    .build();

            this.keyVaultSecretClient = new SecretClientBuilder()
                    .vaultUrl(kvUri)
                    .credential(clientSecretCredential)
                    .buildClient();
        }
    }
}

