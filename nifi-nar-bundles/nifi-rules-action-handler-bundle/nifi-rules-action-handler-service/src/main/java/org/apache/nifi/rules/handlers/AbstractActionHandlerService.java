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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.PropertyContextActionHandler;

import java.util.Map;

public abstract class AbstractActionHandlerService extends AbstractControllerService implements PropertyContextActionHandler {

    protected String enforceActionType;

    public static enum DebugLevels {
        trace, debug, info, warn, error
    }

    public static final PropertyDescriptor ENFORCE_ACTION_TYPE = new PropertyDescriptor.Builder()
            .name("action-handler-enforce-type")
            .displayName("Enforce Action Type")
            .required(false)
            .description("The Action Type that should be supported by this handler.  If provided any other type an " +
                    "exception will be thrown")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public void execute(Action action, Map<String, Object> facts){
        if(actionTypeNotSupported(action)){
            throw new UnsupportedOperationException("This Action Handler does not support actions with the provided type: " + action.getType());
        }
    }

    public void execute(PropertyContext context, Action action, Map<String, Object> facts) {
        execute(action, facts);
    }

    protected boolean actionTypeNotSupported(Action action) {
        return StringUtils.isNotBlank(enforceActionType) && !action.getType().equalsIgnoreCase(enforceActionType);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        enforceActionType = context.getProperty(ENFORCE_ACTION_TYPE).getValue();
    }


}
