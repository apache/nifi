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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.PropertyContextActionHandler;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractActionHandlerService extends AbstractControllerService implements PropertyContextActionHandler {

    protected List<String> enforceActionTypes;
    protected EnforceActionTypeLevel enforceActionTypeLevel;

    public enum DebugLevels {
        trace, debug, info, warn, error
    }

    public enum EnforceActionTypeLevel {
        IGNORE, WARN, EXCEPTION
    }

    public static final PropertyDescriptor ENFORCE_ACTION_TYPE = new PropertyDescriptor.Builder()
            .name("action-handler-enforce-type")
            .displayName("Enforce Action Type")
            .required(false)
            .description("The Action Type(s) that should be supported by this handler.  If provided any other type an " +
                    "exception will be thrown.  This can support a comma delimited list of types (e.g. ALERT,LOG)")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ENFORCE_ACTION_TYPE_LEVEL = new PropertyDescriptor.Builder()
            .name("action-handler-enforce-type-level")
            .displayName("Enforce Level")
            .required(false)
            .description("If specific action types are enforced, this setting specifies whether the action should be ignored," +
                    " a warning should be logged or if an exception is thrown. Default is to ignore the received action.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(EnforceActionTypeLevel.values())
            .defaultValue("IGNORE")
            .build();

    public void execute(Action action, Map<String, Object> facts) {
        if (actionTypeNotSupported(action)) {
            handleActionEnforcement(action);
        } else {
            executeAction(action, facts);
        }
    }

    public void execute(PropertyContext context, Action action, Map<String, Object> facts) {
        if (actionTypeNotSupported(action)) {
            handleActionEnforcement(action);
        } else {
            executeAction(context, action, facts);
        }
    }

    protected void executeAction(Action action, Map<String, Object> facts) {
        throw new UnsupportedOperationException("This method is not supported by this handler.");
    }

    protected void executeAction(PropertyContext propertyContext, Action action, Map<String, Object> facts) {
        throw new UnsupportedOperationException("This method is not supported by this handler");
    }

    protected boolean actionTypeNotSupported(Action action) {
        return enforceActionTypes != null && !enforceActionTypes.contains(action.getType());
    }

    protected void handleActionEnforcement(Action action) {
        String message = "This Action Handler does not support actions with the provided type: " + action.getType();
        if (enforceActionTypeLevel.equals(EnforceActionTypeLevel.WARN)) {
            getLogger().warn(message);
        } else if (enforceActionTypeLevel.equals(EnforceActionTypeLevel.EXCEPTION)) {
            throw new UnsupportedOperationException(message);
        } else if (getLogger().isDebugEnabled()) {
            getLogger().debug(message);
        }
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        String actionTypes = context.getProperty(ENFORCE_ACTION_TYPE).evaluateAttributeExpressions().getValue();
        if(StringUtils.isNotEmpty(actionTypes)){
            enforceActionTypes = Arrays.stream(actionTypes.split(","))
                                       .map(String::trim)
                                       .filter(StringUtils::isNotEmpty)
                                       .collect(Collectors.toList());
        }
        String level = context.getProperty(ENFORCE_ACTION_TYPE_LEVEL).getValue();
        if(StringUtils.isNotEmpty(level)) {
            enforceActionTypeLevel = EnforceActionTypeLevel.valueOf(level);
        }
    }


}
