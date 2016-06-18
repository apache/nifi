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
package org.apache.nifi.update.attributes;

import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardExpressionLanguageCompiler;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.update.attributes.dto.ActionDTO;
import org.apache.nifi.update.attributes.dto.ConditionDTO;
import org.apache.nifi.update.attributes.dto.RuleDTO;

/**
 *
 */
public class UpdateAttributeModelFactory {

    private final VariableRegistry variableRegistry;

    public UpdateAttributeModelFactory(VariableRegistry variableRegistry) {
        this.variableRegistry = variableRegistry;
    }

    public Rule createRule(final RuleDTO dto) {
        if (dto == null) {
            throw new IllegalArgumentException("Rule must be specified.");
        }
        if (dto.getName() == null) {
            throw new IllegalArgumentException("Rule name must be specified.");
        }
        if (dto.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Rule name cannot be blank.");
        }

        final Rule rule = new Rule();
        rule.setId(dto.getId());
        rule.setName(dto.getName());
        rule.setConditions(createConditions(dto.getConditions()));
        rule.setActions(createActions(dto.getActions()));
        return rule;
    }

    public Set<Condition> createConditions(final Set<ConditionDTO> conditionDtos) {
        if (conditionDtos == null) {
            throw new IllegalArgumentException("Conditions must be specified.");
        }

        final Set<Condition> conditions = new HashSet<>(conditionDtos.size());
        for (final ConditionDTO condition : conditionDtos) {
            conditions.add(createCondition(condition));
        }
        return conditions;
    }

    public Condition createCondition(final ConditionDTO dto) {
        if (dto == null) {
            throw new IllegalArgumentException("Condition must be specified.");
        }
        if (dto.getExpression() == null) {
            throw new IllegalArgumentException("Conditions: Expression must be specified.");
        }

        // validate the condition's expression
        final StandardExpressionLanguageCompiler elCompiler = new StandardExpressionLanguageCompiler(variableRegistry);
        final String syntaxError = elCompiler.validateExpression(dto.getExpression(), false);
        if (syntaxError != null) {
            throw new IllegalArgumentException(syntaxError);
        }

        final ResultType resultType = elCompiler.getResultType(dto.getExpression());
        if (!ResultType.BOOLEAN.equals(resultType)) {
            throw new IllegalArgumentException("Return type of condition is " + resultType + " but expected type BOOLEAN");
        }

        final Condition condition = new Condition();
        condition.setId(dto.getId());
        condition.setExpression(dto.getExpression());
        return condition;
    }

    public Set<Action> createActions(final Set<ActionDTO> actionDtos) {
        if (actionDtos == null) {
            throw new IllegalArgumentException("Actions must be specified.");
        }

        final Set<Action> actions = new HashSet<>(actionDtos.size());
        for (final ActionDTO action : actionDtos) {
            actions.add(createAction(action));
        }
        return actions;
    }

    public Action createAction(final ActionDTO dto) {
        if (dto == null) {
            throw new IllegalArgumentException("Action must be specified.");
        }
        if (dto.getAttribute() == null) {
            throw new IllegalArgumentException("Actions: Attribute name must be specified.");
        }
        if (dto.getValue() == null) {
            throw new IllegalArgumentException(String.format("Actions: Value for attribute '%s' must be specified.", dto.getAttribute()));
        }

        try {
            Query.validateExpression(dto.getValue(), true);
        } catch (final AttributeExpressionLanguageParsingException e) {
            throw new IllegalArgumentException("Invalid Expression: " + e.toString(), e);
        }

        final Action action = new Action();
        action.setId(dto.getId());
        action.setAttribute(dto.getAttribute());
        action.setValue(dto.getValue());
        return action;
    }
}
