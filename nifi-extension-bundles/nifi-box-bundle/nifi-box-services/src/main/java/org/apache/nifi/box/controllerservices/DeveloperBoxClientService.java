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
package org.apache.nifi.box.controllerservices;

import com.box.sdk.BoxAPIConnection;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;

@CapabilityDescription("Provides Box client objects through which Box API calls can be used. This using a developer token and is for testing only.")
@Tags({"box", "client", "provider"})
public class DeveloperBoxClientService extends AbstractControllerService implements BoxClientService, VerifiableControllerService {

    public static final PropertyDescriptor DEVELOPER_TOKEN = new PropertyDescriptor.Builder()
            .name("Developer Token")
            .description("The Developer Token to use to interact with the Box API. This is for testing only and should not be used in production.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(DEVELOPER_TOKEN);

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext configurationContext, final ComponentLog componentLog, final Map<String, String> map) {

        final List<ConfigVerificationResult> results = new ArrayList<>();
        try {
            createBoxApiConnection(configurationContext);
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName("Authentication")
                            .outcome(SUCCESSFUL)
                            .explanation("Developer Token verified")
                            .build()
            );
        } catch (final Exception e) {
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName("Authentication")
                            .outcome(FAILED)
                            .explanation("Developer Token failed to verify: " + e.getMessage())
                            .build()
            );
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        boxAPIConnection = createBoxApiConnection(context);
    }

    @Override
    public BoxAPIConnection getBoxApiConnection() {
        return boxAPIConnection;
    }

    private BoxAPIConnection createBoxApiConnection(ConfigurationContext context) {
        final String devToken = context.getProperty(DEVELOPER_TOKEN).evaluateAttributeExpressions().getValue();
        return new BoxAPIConnection(devToken);
    }
}
