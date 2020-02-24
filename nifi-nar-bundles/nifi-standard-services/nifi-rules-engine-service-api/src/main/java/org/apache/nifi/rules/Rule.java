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
package org.apache.nifi.rules;

import java.util.ArrayList;
import java.util.List;

/**
 *  A Rule that defines a condition that should be met by a set of facts in order to perform
 *  one or more {@link Action}
 */

public class Rule implements Cloneable{
    private String name;
    private String description;
    private Integer priority;
    private String condition;
    private List<Action> actions;
    private List<String> facts;

    public Rule() {
    }

    public Rule(String name, String description, Integer priority, String condition, List<Action> actions, List<String> facts) {
        this.name = name;
        this.description = description;
        this.priority = priority;
        this.condition = condition;
        this.actions = actions;
        this.facts = facts;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public List<Action> getActions() {
        return actions;
    }

    public void setActions(List<Action> actions) {
        this.actions = actions;
    }

    public List<String> getFacts() {
        return facts;
    }

    public void setFacts(List<String> facts) {
        this.facts = facts;
    }

    @Override
    public Rule clone(){
        Rule rule = new Rule();
        rule.setName(name);
        rule.setDescription(description);
        rule.setPriority(priority);
        rule.setCondition(condition);

        if (actions != null) {
            final List<Action> actionList = new ArrayList<>();
            rule.setActions(actionList);
            actions.forEach(action -> actionList.add((Action)action.clone()));
        }
        if (facts != null){
            final List<String> factList = new ArrayList<>();
            rule.setFacts(factList);
            factList.addAll(facts);
        }
        return rule;
    }
}
