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

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Rule;
import org.apache.nifi.rules.RulesFactory;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractEasyRulesEngineController extends AbstractControllerService {

    static final AllowableValue YAML = new AllowableValue("YAML", "YAML", "YAML file configuration type.");
    static final AllowableValue JSON = new AllowableValue("JSON", "JSON", "JSON file configuration type.");
    static final AllowableValue NIFI = new AllowableValue("NIFI", "NIFI", "NiFi rules formatted file.");
    static final AllowableValue MVEL = new AllowableValue("MVEL", "Easy Rules MVEL", "Easy Rules File format using MVFLEX Expression Language");
    static final AllowableValue SPEL = new AllowableValue("SPEL", "Easy Rules SpEL", "Easy Rules File format using Spring Expression Language");

    static final PropertyDescriptor RULES_FILE_PATH = new PropertyDescriptor.Builder()
            .name("rules-file-path")
            .displayName("Rules File Path")
            .description("Path to location of rules file. Only one of Rules File or Rules Body may be used")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor RULES_BODY = new PropertyDescriptor.Builder()
            .name("rules-body")
            .displayName("Rules Body")
            .description("Body of rules file to execute. Only one of Rules File or Rules Body may be used")
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor RULES_FILE_TYPE = new PropertyDescriptor.Builder()
            .name("rules-file-type")
            .displayName("Rules File Type")
            .description("File or Body type for rules definition. Supported types are YAML and JSON")
            .required(true)
            .allowableValues(JSON,YAML)
            .defaultValue(JSON.getValue())
            .build();

    static final PropertyDescriptor RULES_FILE_FORMAT = new PropertyDescriptor.Builder()
            .name("rules-file-format")
            .displayName("Rules File Format")
            .description("Format for rules. Supported formats are NiFi Rules, Easy Rules files with MVEL Expression Language" +
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

    static final PropertyDescriptor FILTER_RULES_MISSING_FACTS = new PropertyDescriptor.Builder()
            .name("rules-filter-missing-facts")
            .displayName("Filter Rules With Missing Facts")
            .description("When set to true, the rules engine will first filter out any rule where fact are not available before " +
                    "executing a check or firing that rule. When running a check rules this will return only rules " +
                    "that were evaluated after filtering. NOTE: This is only applicable for the NIFI Rules Format (which allows" +
                    " specification of fact variables) and will be ignored for other formats.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    protected List<PropertyDescriptor> properties;
    protected List<Rule> rules;
    protected volatile String rulesFileFormat;
    protected boolean ignoreConditionErrors;
    protected boolean filterRules;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RULES_FILE_TYPE);
        properties.add(RULES_FILE_PATH);
        properties.add(RULES_BODY);
        properties.add(RULES_FILE_FORMAT);
        properties.add(IGNORE_CONDITION_ERRORS);
        properties.add(FILTER_RULES_MISSING_FACTS);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String rulesFile = context.getProperty(RULES_FILE_PATH).getValue();
        final String rulesBody = context.getProperty(RULES_BODY).getValue();
        final String rulesFileType = context.getProperty(RULES_FILE_TYPE).getValue();
        rulesFileFormat = context.getProperty(RULES_FILE_FORMAT).getValue();
        ignoreConditionErrors = context.getProperty(IGNORE_CONDITION_ERRORS).asBoolean();
        filterRules = context.getProperty(FILTER_RULES_MISSING_FACTS).asBoolean();

        try{
            if(StringUtils.isEmpty(rulesFile)){
                rules = RulesFactory.createRulesFromString(rulesBody, rulesFileType, rulesFileFormat);
            }else{
                rules = RulesFactory.createRulesFromFile(rulesFile, rulesFileType, rulesFileFormat);
            }
        } catch (Exception fex){
            throw new InitializationException(fex);
        }
    }

    /**
     * Custom validation for ensuring exactly one of Script File or Script Body is populated
     *
     * @param validationContext provides a mechanism for obtaining externally
     *                          managed values, such as property values and supplies convenience methods
     *                          for operating on those values
     * @return A collection of validation results
     */
    @Override
    public Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Verify that exactly one of "script file" or "script body" is set
        Map<PropertyDescriptor, String> propertyMap = validationContext.getProperties();
        if (StringUtils.isEmpty(propertyMap.get(RULES_FILE_PATH)) == StringUtils.isEmpty(propertyMap.get(RULES_BODY))) {
            results.add(new ValidationResult.Builder().subject("Rules Body or Rules File").valid(false).explanation(
                    "exactly one of Rules File or Rules Body must be set").build());
        }
        return results;
    }

    protected RulesEngine getRulesEngine() {
        List<Rule> rulesCopy = new ArrayList<>();
        rules.forEach(rule -> {
            rulesCopy.add(rule.clone());
        });
        return new EasyRulesEngine(rulesFileFormat, ignoreConditionErrors, filterRules, Collections.unmodifiableList(rulesCopy));
    }

}
