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
package org.apache.nifi.rules.handlers;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.PropertyContextActionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({"rules", "rules engine", "action", "action handler","lookup"})
@CapabilityDescription("Provides an Action Handler that can be used to dynamically select another Action Handler. " +
"This service will allow multiple ActionHandlers to be defined and registered by action type.  When actions are provided the handlers can " +
"be dynamically determined and executed at runtime.")
@DynamicProperty(name = "actionType ", value = "Action Handler Service", expressionLanguageScope = ExpressionLanguageScope.NONE, description = "")
public class ActionHandlerLookup extends AbstractActionHandlerService{

    private volatile Map<String, PropertyContextActionHandler> actionHandlerMap;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("The Action handler to return when action type = '" + propertyDescriptorName + "'")
                .identifiesControllerService(PropertyContextActionHandler.class)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        int numDefinedServices = 0;
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                numDefinedServices++;
            }

            final String referencedId = context.getProperty(descriptor).getValue();
            if (this.getIdentifier().equals(referencedId)) {
                results.add(new ValidationResult.Builder()
                        .subject(descriptor.getDisplayName())
                        .explanation("the current service cannot be registered as an ActionHandler to lookup")
                        .valid(false)
                        .build());
            }
        }

        if (numDefinedServices == 0) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("at least one Action Handler must be defined via dynamic properties")
                    .valid(false)
                    .build());
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String,PropertyContextActionHandler> serviceMap = new HashMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final PropertyContextActionHandler propertyContextActionHandler = context.getProperty(descriptor).asControllerService(PropertyContextActionHandler.class);
                serviceMap.put(descriptor.getName(), propertyContextActionHandler);
            }
        }

        actionHandlerMap = Collections.unmodifiableMap(serviceMap);
    }

    @OnDisabled
    public void onDisabled() {
        actionHandlerMap = null;
    }

    @Override
    public void execute(Action action, Map<String, Object> facts) {
        execute(null,action,facts);
    }

    @Override
    public void execute(PropertyContext context, Action action, Map<String, Object> facts) {
        PropertyContextActionHandler actionHandler = actionHandlerMap.get(action.getType());
        if (actionHandler == null) {
            throw new ProcessException("No Action Handler was found for Action Type:" + action.getType());
        }
        actionHandler.execute(context,action,facts);
    }
}
