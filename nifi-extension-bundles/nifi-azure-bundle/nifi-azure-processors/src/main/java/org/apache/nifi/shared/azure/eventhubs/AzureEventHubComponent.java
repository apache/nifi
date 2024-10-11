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
package org.apache.nifi.shared.azure.eventhubs;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

/**
 * Azure Event Hub Component interface with shared properties
 */
public interface AzureEventHubComponent {
    PropertyDescriptor TRANSPORT_TYPE = new PropertyDescriptor.Builder()
            .name("Transport Type")
            .displayName("Transport Type")
            .description("Advanced Message Queuing Protocol Transport Type for communication with Azure Event Hubs")
            .allowableValues(AzureEventHubTransportType.class)
            .defaultValue(AzureEventHubTransportType.AMQP)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.HTTP_AUTH};
    PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS))
            .dependsOn(TRANSPORT_TYPE, AzureEventHubTransportType.AMQP_WEB_SOCKETS)
            .build();
}
