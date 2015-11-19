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
package org.apache.nifi.processors.attributes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.apache.nifi.update.attributes.Action;
import org.apache.nifi.update.attributes.Condition;
import org.apache.nifi.update.attributes.Criteria;
import org.apache.nifi.update.attributes.FlowFilePolicy;
import org.apache.nifi.update.attributes.Rule;
import org.apache.nifi.update.attributes.serde.CriteriaSerDe;

/**
 * This processor supports updating flowfile attributes and can do so
 * conditionally or unconditionally.  It can also delete flowfile attributes
 * that match a regular expression.
 *
 * Like the FlowFileMetadataEnhancer, it can
 * be configured with an arbitrary number of optional properties to define how
 * attributes should be updated. Each optional property represents an action
 * that is applied to all incoming flow files. An action is comprised of an
 * attribute key and a format string. The format string supports the following
 * parameters.
 * <ul>
 * <li>%1 - is the random generated UUID. </li>
 * <li>%2 - is the current calendar time. </li>
 * <li>${"attribute.key") - is the flow file attribute value of the key
 * contained within the brackets.</li>
 * </ul>
 *
 * When creating the optional properties, enter the attribute key as the
 * property name and the desired format string as the value. The optional
 * properties are considered default actions and are applied unconditionally.
 *
 * In addition to the default actions, this processor has a user interface (UI)
 * where conditional actions can be specified. In the UI, rules can be created.
 * Rules are comprised of an arbitrary number of conditions and actions. In
 * order for a rule to be activated, all conditions must evaluate to true.
 *
 * A rule condition is comprised of an attribute key and a regular expression. A
 * condition evaluates to true when the flowfile contains the attribute
 * specified and it's value matches the specified regular expression.
 *
 * A rule action follows the same definition as a rule above. It includes an
 * attribute key and a format string. The format string supports the same
 * parameters defined above.
 *
 * When a rule is activated (because conditions evaluate to true), all actions
 * in that rule are executed. Once each action has been applied, any remaining
 * default actions will be applied. This means that if rule action and a default
 * action modify the same attribute, only the rule action will execute. Default
 * actions will only execute when the attribute in question is not modified as
 * part of an activated rule.
 *
 * The incoming flow file is cloned for each rule that is activated. If no rule
 * is activated, any default actions are applied to the original flowfile and it
 * is transferred.
 *
 * This processor only supports a SUCCESS relationship.
 *
 * Note: In order for configuration changes made in the custom UI to take
 * effect, the processor must be stopped and started.
 */
@EventDriven
@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"attributes", "modification", "update", "delete", "Attribute Expression Language"})
@CapabilityDescription("Updates the Attributes for a FlowFile by using the Attribute Expression Language and/or deletes the attributes based on a regular expression")
@DynamicProperty(name = "A FlowFile attribute to update", value = "The value to set it to", supportsExpressionLanguage = true,
        description = "Updates a FlowFile attribute specified by the Dynamic Property's key with the value specified by the Dynamic Property's value")
@WritesAttribute(attribute = "See additional details", description = "This processor may write or remove zero or more attributes as described in additional details")
public class UpdateAttribute extends AbstractProcessor implements Searchable {

    private final AtomicReference<Criteria> criteriaCache = new AtomicReference<>(null);
    private final ConcurrentMap<String, PropertyValue> propertyValues = new ConcurrentHashMap<>();

    private final Set<Relationship> relationships;

    private static final Validator DELETE_PROPERTY_VALIDATOR = new Validator() {
        private final Validator DPV_RE_VALIDATOR = StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true);
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                final AttributeExpression.ResultType resultType = context.newExpressionLanguageCompiler().getResultType(input);
                if (!resultType.equals(AttributeExpression.ResultType.STRING)) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("Expected property to to return type " + AttributeExpression.ResultType.STRING +
                                    " but expression returns type " + resultType)
                            .build();
                }
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(true)
                        .explanation("Property returns type " + AttributeExpression.ResultType.STRING)
                        .build();
            }

            return DPV_RE_VALIDATOR.validate(subject, input, context);
        }
    };

    // static properties
    public static final PropertyDescriptor DELETE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Delete Attributes Expression")
            .description("Regular expression for attributes to be deleted from flowfiles.")
            .required(false)
            .addValidator(DELETE_PROPERTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All FlowFiles are routed to this relationship").name("success").build();

    public UpdateAttribute() {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DELETE_ATTRIBUTES);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void clearPropertyValueMap() {
        propertyValues.clear();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> reasons = new ArrayList<>(super.customValidate(context));

        Criteria criteria = null;
        try {
            criteria = CriteriaSerDe.deserialize(context.getAnnotationData());
        } catch (IllegalArgumentException iae) {
            reasons.add(new ValidationResult.Builder().valid(false).explanation("Unable to deserialize the update criteria." + iae.getMessage()).build());
        }

        // if there is criteria, validate it
        if (criteria != null) {
            final List<Rule> rules = criteria.getRules();

            if (rules == null) {
                reasons.add(new ValidationResult.Builder().valid(false).explanation("Update criteria has been specified by no rules were found.").build());
            } else {
                // validate the each rule
                for (final Rule rule : rules) {
                    if (rule.getName() == null || rule.getName().trim().isEmpty()) {
                        reasons.add(new ValidationResult.Builder().valid(false).explanation("A rule name was not specified.").build());
                    }

                    // validate each condition
                    final Set<Condition> conditions = rule.getConditions();
                    if (conditions == null) {
                        reasons.add(new ValidationResult.Builder().valid(false).explanation(String.format("No conditions for rule '%s' found.", rule.getName())).build());
                    } else {
                        for (final Condition condition : conditions) {
                            if (condition.getExpression() == null) {
                                reasons.add(new ValidationResult.Builder().valid(false).explanation(String.format("No expression for a condition in rule '%s' was found.", rule.getName())).build());
                            } else {
                                final String expression = condition.getExpression().trim();
                                reasons.add(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.BOOLEAN, false)
                                        .validate(String.format("Condition for rule '%s'.", rule.getName()), expression, context));
                            }
                        }
                    }

                    // validate each action
                    final Set<Action> actions = rule.getActions();
                    if (actions == null) {
                        reasons.add(new ValidationResult.Builder().valid(false).explanation(String.format("No actions for rule '%s' found.", rule.getName())).build());
                    } else {
                        for (final Action action : actions) {
                            if (action.getAttribute() == null) {
                                reasons.add(new ValidationResult.Builder().valid(false).explanation(String.format("An action in rule '%s' is missing the attribute name.", rule.getName())).build());
                            } else if (action.getValue() == null) {
                                reasons.add(new ValidationResult.Builder().valid(false)
                                        .explanation(String.format("No value for attribute '%s' in rule '%s' was found.", action.getAttribute(), rule.getName())).build());
                            } else {
                                reasons.add(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true)
                                        .validate(String.format("Action for rule '%s'.", rule.getName()), action.getValue(), context));
                            }
                        }
                    }
                }
            }
        }

        return reasons;
    }

    @Override
    public Collection<SearchResult> search(final SearchContext context) {
        final String term = context.getSearchTerm();

        final Collection<SearchResult> results = new ArrayList<>();
        if (StringUtils.isBlank(context.getAnnotationData())) {
            return results;
        }

        try {
            // parse the annotation data
            final Criteria criteria = CriteriaSerDe.deserialize(context.getAnnotationData());

            // ensure there are some rules
            if (criteria.getRules() != null) {
                final FlowFilePolicy flowFilePolicy = criteria.getFlowFilePolicy();
                if (flowFilePolicy != null && StringUtils.containsIgnoreCase(flowFilePolicy.name(), term)) {
                    results.add(new SearchResult.Builder().label("FlowFile policy").match(flowFilePolicy.name()).build());
                }

                for (final Rule rule : criteria.getRules()) {
                    if (StringUtils.containsIgnoreCase(rule.getName(), term)) {
                        results.add(new SearchResult.Builder().label("Rule name").match(rule.getName()).build());
                    }

                    // ensure there are some conditions
                    if (rule.getConditions() != null) {
                        for (final Condition condition : rule.getConditions()) {
                            if (StringUtils.containsIgnoreCase(condition.getExpression(), term)) {
                                results.add(new SearchResult.Builder().label(String.format("Condition in rule '%s'", rule.getName())).match(condition.getExpression()).build());
                            }
                        }
                    }

                    // ensure there are some actions
                    if (rule.getActions() != null) {
                        for (final Action action : rule.getActions()) {
                            if (StringUtils.containsIgnoreCase(action.getAttribute(), term)) {
                                results.add(new SearchResult.Builder().label(String.format("Action in rule '%s'", rule.getName())).match(action.getAttribute()).build());
                            }
                            if (StringUtils.containsIgnoreCase(action.getValue(), term)) {
                                results.add(new SearchResult.Builder().label(String.format("Action in rule '%s'", rule.getName())).match(action.getValue()).build());
                            }
                        }
                    }
                }
            }

            return results;
        } catch (Exception e) {
            return results;
        }
    }

    @OnScheduled
    public void parseAnnotationData(final ProcessContext context) {
        criteriaCache.set(CriteriaSerDe.deserialize(context.getAnnotationData()));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final ProcessorLog logger = getLogger();
        final Criteria criteria = criteriaCache.get();

        List<FlowFile> flowFiles = session.get(100);
        if (flowFiles.isEmpty()) {
            return;
        }

        final Map<PropertyDescriptor, String> properties = context.getProperties();

        // get the default actions
        final Map<String, Action> defaultActions = getDefaultActions(properties);

        // record which rule should be applied to which flow file - when operating
        // in 'use clone' mode, this collection will contain a number of entries
        // that map to single element lists. this is because the original flowfile
        // is cloned for each matching rule. in 'use original' mode, this collection
        // will contain a single entry that maps a list of multiple rules. this is
        // because is the original flowfile is used for all matching rules. in this
        // case the order of the matching rules is perserved in the list
        final Map<FlowFile, List<Rule>> matchedRules = new HashMap<>();

        for (FlowFile flowFile : flowFiles) {
            matchedRules.clear();

            // if there is update criteria specified, evaluate it
            if (criteria != null && evaluateCriteria(session, context, criteria, flowFile, matchedRules)) {
                // apply the actions for each rule and transfer the flowfile
                for (final Map.Entry<FlowFile, List<Rule>> entry : matchedRules.entrySet()) {
                    FlowFile match = entry.getKey();
                    final List<Rule> rules = entry.getValue();

                    // execute each matching rule(s)
                    match = executeActions(session, context, rules, defaultActions, match);
                    logger.info("Updated attributes for {}; transferring to '{}'", new Object[]{match, REL_SUCCESS.getName()});

                    // transfer the match
                    session.getProvenanceReporter().modifyAttributes(match);
                    session.transfer(match, REL_SUCCESS);
                }
            } else {
                // transfer the flowfile to no match (that has the default actions applied)
                flowFile = executeActions(session, context, null, defaultActions, flowFile);
                logger.info("Updated attributes for {}; transferring to '{}'", new Object[]{flowFile, REL_SUCCESS.getName()});
                session.getProvenanceReporter().modifyAttributes(flowFile);
                session.transfer(flowFile, REL_SUCCESS);
            }
        }
    }

    //Evaluates the specified Criteria on the specified flowfile. Clones the
    // specified flow file for each rule that is applied.
    private boolean evaluateCriteria(final ProcessSession session, final ProcessContext context, final Criteria criteria, final FlowFile flowfile, final Map<FlowFile, List<Rule>> matchedRules) {
        final ProcessorLog logger = getLogger();
        final List<Rule> rules = criteria.getRules();

        // consider each rule and hold a copy of the flowfile for each matched rule
        for (final Rule rule : rules) {
            // evaluate the rule
            if (evaluateRule(context, rule, flowfile)) {
                final FlowFile flowfileToUse;

                // determine if we should use the original flow file or clone
                if (FlowFilePolicy.USE_ORIGINAL.equals(criteria.getFlowFilePolicy()) || matchedRules.isEmpty()) {
                    flowfileToUse = flowfile;
                } else {
                    // clone the original for this rule
                    flowfileToUse = session.clone(flowfile);
                }

                // store the flow file to use when executing this rule
                List<Rule> rulesForFlowFile = matchedRules.get(flowfileToUse);
                if (rulesForFlowFile == null) {
                    rulesForFlowFile = new ArrayList<>();
                    matchedRules.put(flowfileToUse, rulesForFlowFile);
                }
                rulesForFlowFile.add(rule);

                // log if appropriate
                if (logger.isDebugEnabled()) {
                    logger.debug(this + " all conditions met for rule '" + rule.getName() + "'. Using flow file - " + flowfileToUse);
                }
            }
        }

        return !matchedRules.isEmpty();
    }

    //Evaluates the specified rule on the specified flowfile.
    private boolean evaluateRule(final ProcessContext context, final Rule rule, FlowFile flowfile) {
        // go through each condition
        for (final Condition condition : rule.getConditions()) {

            // fail if any condition is not met
            if (!evaluateCondition(context, condition, flowfile)) {
                return false;
            }
        }

        return true;
    }

    private PropertyValue getPropertyValue(final String text, final ProcessContext context) {
        PropertyValue currentValue = propertyValues.get(text);
        if (currentValue == null) {
            currentValue = context.newPropertyValue(text);
            PropertyValue previousValue = propertyValues.putIfAbsent(text, currentValue);
            if (previousValue != null) {
                currentValue = previousValue;
            }
        }

        return currentValue;
    }

    //Evaluates the specified condition on the specified flowfile.
    private boolean evaluateCondition(final ProcessContext context, final Condition condition, final FlowFile flowfile) {
        try {
            // evaluate the expression for the given flow file
            return getPropertyValue(condition.getExpression(), context).evaluateAttributeExpressions(flowfile).asBoolean();
        } catch (final ProcessException pe) {
            throw new ProcessException(String.format("Unable to evaluate condition '%s': %s.", condition.getExpression(), pe), pe);
        }
    }

    // Executes the specified action on the specified flowfile.
    private FlowFile executeActions(final ProcessSession session, final ProcessContext context, final List<Rule> rules, final Map<String, Action> defaultActions, final FlowFile flowfile) {
        final ProcessorLog logger = getLogger();
        final Map<String, Action> actions = new HashMap<>(defaultActions);
        final String ruleName = (rules == null || rules.isEmpty()) ? "default" : rules.get(rules.size() - 1).getName();

        // if a rule matched, get its actions and possible overwrite the default ones
        if (rules != null && rules.size() > 0) {
            // combine all rules actions with the default actions... loop through the rules in order, this way
            // subsequent matching rules will take precedence over previously matching rules and default values
            for (final Rule rule : rules) {
                for (final Action action : rule.getActions()) {
                    // store the action and overwrite the previous value (from the defaults or a previously matching rule)
                    actions.put(action.getAttribute(), action);
                }
            }

            // add an action for the matched rule - when matching multiple rules against
            // the original flowfile (use original) this will leave the last matching
            // rule's name as the value of this attribute. this decision was made since
            // this would be the behavior if they user chained multiple UpdateAttributes
            // together with 'use clone' specified
            final Action matchedRuleAction = new Action();
            matchedRuleAction.setAttribute(getClass().getSimpleName() + ".matchedRule");
            matchedRuleAction.setValue(ruleName);
            actions.put(matchedRuleAction.getAttribute(), matchedRuleAction);
        }

        // attribute values that will be applied to the flow file
        final Map<String, String> attributesToUpdate = new HashMap<>(actions.size());
        final Set<String> attributesToDelete = new HashSet<>(actions.size());

        // go through each action
        for (final Action action : actions.values()) {
            if (!action.getAttribute().equals(DELETE_ATTRIBUTES.getName())) {
                try {
                    final String newAttributeValue = getPropertyValue(action.getValue(), context).evaluateAttributeExpressions(flowfile).getValue();

                    // log if appropriate
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("%s setting attribute '%s' = '%s' for %s per rule '%s'.", this, action.getAttribute(), newAttributeValue, flowfile, ruleName));
                    }

                    attributesToUpdate.put(action.getAttribute(), newAttributeValue);
                } catch (final ProcessException pe) {
                    throw new ProcessException(String.format("Unable to evaluate new value for attribute '%s': %s.", action.getAttribute(), pe), pe);
                }
            } else {
                try {
                    final String actionValue = action.getValue();
                    final String regex = (actionValue == null) ? null :
                            getPropertyValue(actionValue, context).evaluateAttributeExpressions(flowfile).getValue();
                    if (regex != null) {
                        Pattern pattern = Pattern.compile(regex);
                        final Set<String> attributeKeys = flowfile.getAttributes().keySet();
                        for (final String key : attributeKeys) {
                            if (pattern.matcher(key).matches()) {

                                // log if appropriate
                                if (logger.isDebugEnabled()) {
                                    logger.debug(String.format("%s deleting attribute '%s' for %s per regex '%s'.", this,
                                            key, flowfile, regex));
                                }

                                attributesToDelete.add(key);
                            }
                        }
                    }
                } catch (final ProcessException pe) {
                    throw new ProcessException(String.format("Unable to delete attribute '%s': %s.", action.getAttribute(), pe), pe);
                }
            }
        }

        // If the 'alternate.identifier' attribute is added, then we want to create an ADD_INFO provenance event.
        final String alternateIdentifierAdd = attributesToUpdate.get(CoreAttributes.ALTERNATE_IDENTIFIER.key());
        if (alternateIdentifierAdd != null) {
            try {
                final URI uri = new URI(alternateIdentifierAdd);
                final String namespace = uri.getScheme();
                if (namespace != null) {
                    final String identifier = alternateIdentifierAdd.substring(Math.min(namespace.length() + 1, alternateIdentifierAdd.length() - 1));
                    session.getProvenanceReporter().associate(flowfile, namespace, identifier);
                }
            } catch (final URISyntaxException e) {
            }
        }

        // update and delete the flowfile attributes
        return session.removeAllAttributes(session.putAllAttributes(flowfile, attributesToUpdate), attributesToDelete);
    }

    // Gets the default actions.
    private Map<String, Action> getDefaultActions(final Map<PropertyDescriptor, String> properties) {
        final Map<String, Action> defaultActions = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            final Action action = new Action();
            action.setAttribute(entry.getKey().getName());
            action.setValue(entry.getValue());
            defaultActions.put(action.getAttribute(), action);
        }

        return defaultActions;
    }
}
