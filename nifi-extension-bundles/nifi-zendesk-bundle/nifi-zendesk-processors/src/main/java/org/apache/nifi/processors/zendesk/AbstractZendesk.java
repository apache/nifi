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
package org.apache.nifi.processors.zendesk;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.common.zendesk.ZendeskAuthenticationContext;
import org.apache.nifi.common.zendesk.ZendeskAuthenticationType;
import org.apache.nifi.common.zendesk.ZendeskClient;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import static org.apache.nifi.common.zendesk.ZendeskProperties.WEB_CLIENT_SERVICE_PROVIDER;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_CREDENTIAL;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_TYPE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_SUBDOMAIN;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_USER;

public abstract class AbstractZendesk extends AbstractProcessor {

    static final String RECORD_COUNT_ATTRIBUTE_NAME = "record.count";

    ZendeskClient zendeskClient;

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful HTTP request.")
            .build();

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        final String user = context.getProperty(ZENDESK_USER).evaluateAttributeExpressions().getValue();
        final ZendeskAuthenticationType authenticationType = context.getProperty(ZENDESK_AUTHENTICATION_TYPE).asAllowableValue(ZendeskAuthenticationType.class);
        final String authenticationCredentials = context.getProperty(ZENDESK_AUTHENTICATION_CREDENTIAL).evaluateAttributeExpressions().getValue();
        final String subdomain = context.getProperty(ZENDESK_SUBDOMAIN).evaluateAttributeExpressions().getValue();
        final ZendeskAuthenticationContext authenticationContext = new ZendeskAuthenticationContext(subdomain, user, authenticationType, authenticationCredentials);
        zendeskClient = new ZendeskClient(webClientServiceProvider, authenticationContext);
    }

    HttpUriBuilder uriBuilder(String resourcePath) {
        return zendeskClient.uriBuilder(resourcePath);
    }

}
