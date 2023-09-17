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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.mvel2.MVEL;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@Tags({"rules", "rules engine", "action", "action handler", "expression language","MVEL","SpEL"})
@CapabilityDescription("Executes an action containing an expression written in MVEL or SpEL. The action " +
"is usually created by a rules engine. Action objects executed with this Handler should contain \"command\" and \"type\" attributes.")
public class ExpressionHandler extends AbstractActionHandlerService {

    enum ExpresssionType {
        MVEL, SPEL;
    }

    public static final PropertyDescriptor DEFAULT_EXPRESSION_LANGUAGE_TYPE = new PropertyDescriptor.Builder()
            .name("default-expression-language-type")
            .displayName("Default Expression Language Type")
            .required(true)
            .description("If an expression language type is not provided as an attribute within an Action, the default expression language that " +
                    "should be used to compile and execute action. Supported languages are MVEL and Spring Expression Language (SpEL).")
            .allowableValues(ExpresssionType.values())
            .defaultValue("MVEL")
            .build();

    private List<PropertyDescriptor> properties;
    private ExpresssionType type;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DEFAULT_EXPRESSION_LANGUAGE_TYPE);
        properties.add(ENFORCE_ACTION_TYPE);
        properties.add(ENFORCE_ACTION_TYPE_LEVEL);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        type = ExpresssionType.valueOf(context.getProperty(DEFAULT_EXPRESSION_LANGUAGE_TYPE).getValue());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void executeAction(PropertyContext propertyContext, Action action, Map<String, Object> facts) {
        executeAction(action, facts);
    }

    @Override
    protected void executeAction(Action action, Map<String, Object> facts) {
        Map<String, String> attributes = action.getAttributes();
        final String command = attributes.get("command");
        if(StringUtils.isNotEmpty(command)) {
            try {
                final String type = attributes.getOrDefault("type",this.type.toString());
                ExpresssionType expresssionType = ExpresssionType.valueOf(type);
                if (expresssionType.equals(ExpresssionType.MVEL)) {
                    executeMVEL(command, facts);
                } else {
                    executeSPEL(command, facts);
                }
            } catch (Exception ex) {
                getLogger().warn("Error occurred when attempting to execute expression. Action: {}, Facts - {}", action, facts, ex);
            }
        }else{
            getLogger().warn("Command attribute was not provided.  Action: {}, Facts - {}",
                    new Object[]{action, facts});
        }
    }

    private void executeMVEL(String command, Map<String, Object> facts) {
        MVEL.executeExpression(MVEL.compileExpression(command), facts);
        if(getLogger().isDebugEnabled()) {
            getLogger().debug("Expression was executed successfully: {}: {}", new Object[]{type, command});
        }
    }

    private void executeSPEL(String command, Map<String, Object> facts) {
        final ExpressionParser parser = new SpelExpressionParser();
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.setRootObject(facts);
        context.setVariables(facts);
        Expression expression = parser.parseExpression(command);
        Object value = expression.getValue(context);
        if(getLogger().isDebugEnabled()) {
            getLogger().debug("Expression was executed successfully with result: {}. {}: {}", new Object[]{value, type, command});
        }
    }

}
