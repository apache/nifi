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
package org.apache.nifi.rules.engine;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.ActionHandler;
import org.apache.nifi.rules.Rule;
import org.apache.nifi.rules.RulesFactory;
import org.apache.nifi.rules.RulesMVELCondition;
import org.apache.nifi.rules.RulesSPELCondition;
import org.jeasy.rules.api.Condition;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.RuleListener;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.core.RuleBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implementation of RulesEngineService interface
 *
 * @see RulesEngineService
 */
@CapabilityDescription("Defines and execute the rules stored in NiFi or EasyRules file formats for a given set of facts. Supports " +
        "rules stored as JSON or YAML file types.")
@Tags({ "rules","rules-engine","engine","actions","facts" })
public class EasyRulesEngineService  extends AbstractControllerService implements RulesEngineService {

    static final AllowableValue YAML = new AllowableValue("YAML", "YAML", "YAML file configuration type.");
    static final AllowableValue JSON = new AllowableValue("JSON", "JSON", "JSON file configuration type.");
    static final AllowableValue NIFI = new AllowableValue("NIFI", "NIFI", "NiFi rules formatted file.");
    static final AllowableValue MVEL = new AllowableValue("MVEL", "Easy Rules MVEL", "Easy Rules File format using MVFLEX Expression Language");
    static final AllowableValue SPEL = new AllowableValue("SPEL", "Easy Rules SpEL", "Easy Rules File format using Spring Expression Language");

    static final PropertyDescriptor RULES_FILE_PATH = new PropertyDescriptor.Builder()
            .name("rules-file-path")
            .displayName("Rules File Path")
            .description("Path to location of rules file.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor RULES_FILE_TYPE = new PropertyDescriptor.Builder()
            .name("rules-file-type")
            .displayName("Rules File Type")
            .description("File type for rules definition. Supported file types are YAML and JSON")
            .required(true)
            .allowableValues(JSON,YAML)
            .defaultValue(JSON.getValue())
            .build();

    static final PropertyDescriptor RULES_FILE_FORMAT = new PropertyDescriptor.Builder()
            .name("rules-file-format")
            .displayName("Rules File Format")
            .description("File format for rules. Supported formats are NiFi Rules, Easy Rules files with MVEL Expression Language" +
                    " and Easy Rules files with Spring Expression Language.")
            .required(true)
            .allowableValues(NIFI,MVEL,SPEL)
            .defaultValue(NIFI.getValue())
            .build();

    static final PropertyDescriptor IGNORE_CONDITION_ERRORS = new PropertyDescriptor.Builder()
            .name("rules-ignore-condition-errors")
            .displayName("Ignore Condition Errors")
            .description("When set to true, rules engine will ignore errors for any rule that encounters issues " +
                    "when compiling rule conditions (including syntax errors and/or missing facts). Rule will simply return as false " +
                    "and engine will continue with execution.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    protected List<PropertyDescriptor> properties;
    protected volatile List<Rule> rules;
    protected volatile String rulesFileFormat;
    private boolean ignoreConditionErrors;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RULES_FILE_TYPE);
        properties.add(RULES_FILE_PATH);
        properties.add(RULES_FILE_FORMAT);
        properties.add(IGNORE_CONDITION_ERRORS);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String rulesFile = context.getProperty(RULES_FILE_PATH).getValue();
        final String rulesFileType = context.getProperty(RULES_FILE_TYPE).getValue();
        rulesFileFormat = context.getProperty(RULES_FILE_FORMAT).getValue();
        ignoreConditionErrors = context.getProperty(IGNORE_CONDITION_ERRORS).asBoolean();
        try{
            rules = RulesFactory.createRules(rulesFile, rulesFileType, rulesFileFormat);
        } catch (Exception fex){
            throw new InitializationException(fex);
        }
    }

    /**
     * Return the list of actions what should be executed for a given set of facts
     * @param facts a Map of key and facts values, as objects, that should be evaluated by the rules engine
     * @return
     */
    @Override
    public List<Action> fireRules(Map<String, Object> facts) {
        final List<Action> actions = new ArrayList<>();
        if (rules == null || facts == null || facts.isEmpty()) {
            return null;
        }else {
            org.jeasy.rules.api.Rules easyRules = convertToEasyRules(rules, (action, eventFacts) ->
                    actions.add(action));
            Facts easyFacts = new Facts();
            facts.forEach(easyFacts::put);
            DefaultRulesEngine rulesEngine = new DefaultRulesEngine();
            rulesEngine.registerRuleListener(new EasyRulesListener());
            rulesEngine.fire(easyRules, easyFacts);
            return actions;
        }
    }


    protected Rules convertToEasyRules(List<Rule> rules, ActionHandler actionHandler) {
        final Rules easyRules = new Rules();
        rules.forEach(rule -> {
            RuleBuilder ruleBuilder = new RuleBuilder();
            Condition condition = rulesFileFormat.equalsIgnoreCase(SPEL.getValue())
                                 ? new RulesSPELCondition(rule.getCondition(), ignoreConditionErrors): new RulesMVELCondition(rule.getCondition(), ignoreConditionErrors);
            ruleBuilder.name(rule.getName())
                    .description(rule.getDescription())
                    .priority(rule.getPriority())
                    .when(condition);
            for (Action action : rule.getActions()) {
                ruleBuilder.then(facts -> {
                    actionHandler.execute(action, facts.asMap());
                });
            }
            easyRules.register(ruleBuilder.build());
        });
        return easyRules;
    }

    private class EasyRulesListener implements RuleListener {
        @Override
        public boolean beforeEvaluate(org.jeasy.rules.api.Rule rule, Facts facts) {
            return true;
        }

        @Override
        public void afterEvaluate(org.jeasy.rules.api.Rule rule, Facts facts, boolean b) {

        }

        @Override
        public void beforeExecute(org.jeasy.rules.api.Rule rule, Facts facts) {

        }

        @Override
        public void onSuccess(org.jeasy.rules.api.Rule rule, Facts facts) {
            getLogger().debug("Rules was successfully processed for: {}",new Object[]{rule.getName()});
        }

        @Override
        public void onFailure(org.jeasy.rules.api.Rule rule, Facts facts, Exception e) {
            getLogger().warn("Rule execution failed for: {}", new Object[]{rule.getName()}, e);
        }
    }

}
