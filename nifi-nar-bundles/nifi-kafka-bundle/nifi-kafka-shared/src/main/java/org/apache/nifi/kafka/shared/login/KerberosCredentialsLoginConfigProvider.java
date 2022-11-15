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
import org.apache.nifi.kerberos.KerberosCredentialsService;

/**
 * Kerberos Login Module implementation of configuration provider
 */
public class KerberosCredentialsLoginConfigProvider implements LoginConfigProvider {
    private static final String MODULE_CLASS_NAME = "com.sun.security.auth.module.Krb5LoginModule";

    private static final String FORMAT = "%s required renewTicket=true useKeyTab=true serviceName=\"%s\" principal=\"%s\" keyTab=\"%s\";";

    /**
     * Get JAAS configuration using configured Kerberos credentials
     *
     * @param context Property Context
     * @return JAAS configuration with Kerberos Login Module
     */
    @Override
    public String getConfiguration(final PropertyContext context) {
        final String principal;
        final String keyTab;

        final KerberosCredentialsService credentialsService = context.getProperty(KafkaClientComponent.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        if (credentialsService == null) {
            principal = context.getProperty(KafkaClientComponent.KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
            keyTab = context.getProperty(KafkaClientComponent.KERBEROS_KEYTAB).evaluateAttributeExpressions().getValue();
        } else {
            principal = credentialsService.getPrincipal();
            keyTab = credentialsService.getKeytab();
        }

        final String serviceName = context.getProperty(KafkaClientComponent.KERBEROS_SERVICE_NAME).evaluateAttributeExpressions().getValue();
        return String.format(FORMAT, MODULE_CLASS_NAME, serviceName, principal, keyTab);
    }
}
