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
import org.apache.nifi.kafka.shared.property.SaslMechanism;

import java.util.Map;

/**
 * Delegating Login Module implementation of configuration provider
 */
public class DelegatingLoginConfigProvider implements LoginConfigProvider {
    private static final LoginConfigProvider SCRAM_PROVIDER = new ScramLoginConfigProvider();

    private static final Map<SaslMechanism, LoginConfigProvider> PROVIDERS = Map.of(
            SaslMechanism.GSSAPI, new KerberosDelegatingLoginConfigProvider(),
            SaslMechanism.PLAIN, new PlainLoginConfigProvider(),
            SaslMechanism.SCRAM_SHA_256, SCRAM_PROVIDER,
            SaslMechanism.SCRAM_SHA_512, SCRAM_PROVIDER,
            SaslMechanism.AWS_MSK_IAM, new AwsMskIamLoginConfigProvider()
    );

    /**
     * Get JAAS configuration using configured username and password properties
     *
     * @param context Property Context
     * @return JAAS configuration with Plain Login Module
     */
    @Override
    public String getConfiguration(final PropertyContext context) {
        final SaslMechanism saslMechanism = context.getProperty(KafkaClientComponent.SASL_MECHANISM).asAllowableValue(SaslMechanism.class);
        final LoginConfigProvider loginConfigProvider = PROVIDERS.getOrDefault(saslMechanism, SCRAM_PROVIDER);
        return loginConfigProvider.getConfiguration(context);
    }
}
