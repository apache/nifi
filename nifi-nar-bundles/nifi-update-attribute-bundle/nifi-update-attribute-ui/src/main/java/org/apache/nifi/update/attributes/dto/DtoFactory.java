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
package org.apache.nifi.update.attributes.dto;

import java.util.Set;
import java.util.TreeSet;
import org.apache.nifi.update.attributes.Condition;
import org.apache.nifi.update.attributes.Action;
import org.apache.nifi.update.attributes.Rule;

/**
 *
 */
public class DtoFactory {

    public static RuleDTO createRuleDTO(final Rule rule) {
        final RuleDTO dto = new RuleDTO();
        dto.setId(rule.getId());
        dto.setName(rule.getName());

        if (rule.getConditions() != null) {
            final Set<ConditionDTO> conditions = new TreeSet<>();
            for (final Condition condition : rule.getConditions()) {
                conditions.add(createConditionDTO(condition));
            }
            dto.setConditions(conditions);
        }

        if (rule.getActions() != null) {
            final Set<ActionDTO> actions = new TreeSet<>();
            for (final Action action : rule.getActions()) {
                actions.add(createActionDTO(action));
            }
            dto.setActions(actions);
        }

        return dto;
    }

    public static ConditionDTO createConditionDTO(final Condition condition) {
        final ConditionDTO dto = new ConditionDTO();
        dto.setId(condition.getId());
        dto.setExpression(condition.getExpression());
        return dto;
    }

    public static ActionDTO createActionDTO(final Action action) {
        final ActionDTO dto = new ActionDTO();
        dto.setId(action.getId());
        dto.setAttribute(action.getAttribute());
        dto.setValue(action.getValue());
        return dto;
    }
}
