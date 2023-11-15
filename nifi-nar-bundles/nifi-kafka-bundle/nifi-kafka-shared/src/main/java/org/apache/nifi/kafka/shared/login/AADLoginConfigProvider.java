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
package org.apache.nifi.kafka.shared.login;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.util.StringUtils;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

public class AADLoginConfigProvider implements LoginConfigProvider {

    private static final String MODULE_CLASS = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";

    public static final String AZURE_TENANT_ID = "azureTenantId";

    public static final String AZURE_APP_ID = "azureAppId";

    public static final String AZURE_APP_SECRET = "azureAppSecret";

    public static final String BOOTSTRAP_SERVER = "bootstrapServer";

    @Override
    public String getConfiguration(PropertyContext context) {

        final LoginConfigBuilder builder = new LoginConfigBuilder(MODULE_CLASS, REQUIRED);

        final String azureTenantId = context.getProperty(KafkaClientComponent.AZURE_TENANT_ID).evaluateAttributeExpressions().getValue();

        final String azureAppId = context.getProperty(KafkaClientComponent.AZURE_APP_ID).evaluateAttributeExpressions().getValue();

        final String azureAppSecret = context.getProperty(KafkaClientComponent.AZURE_APP_SECRET).evaluateAttributeExpressions().getValue();

        final String bootstrapServer = context.getProperty(KafkaClientComponent.BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();

        if (StringUtils.isNotBlank(azureTenantId)
            && StringUtils.isNotBlank(azureAppId)
            && StringUtils.isNotBlank(azureAppSecret)
            && StringUtils.isNotBlank(bootstrapServer)
        ) {
            builder.append(AZURE_TENANT_ID, azureTenantId);
            builder.append(AZURE_APP_ID, azureAppId);
            builder.append(AZURE_APP_SECRET, azureAppSecret);
            builder.append(BOOTSTRAP_SERVER, bootstrapServer);
        }

        return builder.build();
    }

}
