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
package org.apache.nifi.authorization.azure;

import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.StringUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class AbstractAzureUserGroupProvider {
    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 10_000;
    public static final String REFRESH_DELAY_PROPERTY = "Refresh Delay";
    public static final String DEFAULT_REFRESH_DELAY = "5 mins";

    public static final String AUTHORITY_ENDPOINT_PROPERTY = "Authority Endpoint";
    public static final String TENANT_ID_PROPERTY = "Directory ID";
    public static final String APP_REG_CLIENT_ID_PROPERTY = "Application ID";
    public static final String APP_REG_CLIENT_SECRET_PROPERTY = "Client Secret";
    public static final String AZURE_PUBLIC_CLOUD = "https://login.microsoftonline.com/";

    protected ScheduledExecutorService newScheduler(UserGroupProviderInitializationContext initializationContext)
            throws AuthorizerCreationException {
        return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName(String.format("%s (%s) - UserGroup Refresh", getClass().getSimpleName(), initializationContext.getIdentifier()));
                return thread;
            }
        });
    }

    protected ClientCredentialAuthProvider getClientCredentialAuthProvider(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        Function<String, String> getRequiredProperty = name -> {
            final String value = getProperty(configurationContext, name, null);
            if (StringUtils.isBlank(value)) {
                throw new AuthorizerCreationException(String.format("%s is a required field", name));
            }
            return value;
        };

        final String authorityEndpoint = getProperty(configurationContext, AUTHORITY_ENDPOINT_PROPERTY, AZURE_PUBLIC_CLOUD);
        final String tenantId = getRequiredProperty.apply(TENANT_ID_PROPERTY);
        final String clientId = getRequiredProperty.apply(APP_REG_CLIENT_ID_PROPERTY);
        final String clientSecret = getRequiredProperty.apply(APP_REG_CLIENT_SECRET_PROPERTY);

        return new ClientCredentialAuthProvider.Builder()
                .authorityEndpoint(authorityEndpoint)
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();
    }

    protected IGraphServiceClient getGraphServiceClient(ClientCredentialAuthProvider authProvider) {
        return GraphServiceClient.builder().authenticationProvider(authProvider).buildClient();
    }

    static long getDelayProperty(AuthorizerConfigurationContext authContext, String propertyName, String defaultValue) {
        final String propertyValue = getProperty(authContext, propertyName, defaultValue);
        final long syncInterval;
        try {
            syncInterval = Math.round(FormatUtils.getPreciseTimeDuration(propertyValue, TimeUnit.MILLISECONDS));
        } catch (final IllegalArgumentException ignored) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is not a valid time interval.", propertyName, propertyValue));
        }

        if (syncInterval < MINIMUM_SYNC_INTERVAL_MILLISECONDS) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is below the minimum value of '%d ms'", propertyName, propertyValue, MINIMUM_SYNC_INTERVAL_MILLISECONDS));
        }
        return syncInterval;
    }

    static String getProperty(AuthorizerConfigurationContext authContext, String propertyName, String defaultValue) {
        final PropertyValue property = authContext.getProperty(propertyName);

        if (property != null && property.isSet()) {
            final String value = property.getValue();
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }

        return defaultValue;
    }
}
