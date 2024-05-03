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

/**
 * SASL Plain Login Module implementation of configuration provider
 */
public class PlainLoginConfigProvider implements LoginConfigProvider {
    private static final String MODULE_CLASS_NAME = "org.apache.kafka.common.security.plain.PlainLoginModule";

    private static final String FORMAT = "%s required username=\"%s\" password=\"%s\";";

    /**
     * Get JAAS configuration using configured username and password properties
     *
     * @param context Property Context
     * @return JAAS configuration with Plain Login Module
     */
    @Override
    public String getConfiguration(final PropertyContext context) {
        final String username = context.getProperty(KafkaClientComponent.SASL_USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(KafkaClientComponent.SASL_PASSWORD).evaluateAttributeExpressions().getValue();
        return String.format(FORMAT, MODULE_CLASS_NAME, username, password);
    }
}
