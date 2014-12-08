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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Criteria for updating flow file attributes
 */
public class Criteria {

    // note: this class does not need to be synchronized/locked due to 
    // its usage. a new instance is used for getting or updating the
    // rule criteria due to the nature of how annotation data is set.
    // this will be a new instance for each request and the setting of
    // annotation data is protected by a rest api wide write-lock.
    // likewise, the processor uses this class as a simple look up. if 
    // this ever changed (not likely) then we would have to introduce 
    // some thread safety here.
    private Map<String, Rule> rules;
    private FlowFilePolicy flowFilePolicy;

    public Criteria() {
        this(FlowFilePolicy.USE_CLONE, null);
    }

    public Criteria(final FlowFilePolicy flowFilePolicy, final List<Rule> ruleList) {
        this.flowFilePolicy = flowFilePolicy;
        this.rules = new LinkedHashMap<>();

        if (ruleList != null) {
            for (final Rule rule : ruleList) {
                this.rules.put(rule.getId(), rule);
            }
        }
    }

    /**
     * Adds the specified rule to the end of the rule collection.
     *
     * @param rule
     */
    public void addRule(final Rule rule) {
        rules.put(rule.getId(), rule);
    }

    /**
     * Gets the specified rule from the rule collection.
     *
     * @param ruleId
     * @return
     */
    public Rule getRule(final String ruleId) {
        return rules.get(ruleId);
    }

    /**
     * Deletes the specified rule from the rule collection.
     *
     * @param rule
     */
    public void deleteRule(final Rule rule) {
        rules.remove(rule.getId());
    }

    /**
     * Returns the rule ordering.
     *
     * @return
     */
    public List<String> getRuleOrder() {
        return Collections.unmodifiableList(new ArrayList<>(rules.keySet()));
    }

    /**
     * Reorders the rule collection. The specified new rule order must contain
     * the rule id for each rule in the collection.
     *
     * @param newRuleOrder
     */
    public void reorder(final List<String> newRuleOrder) {
        // ensure all known rules are accounted for
        if (newRuleOrder.size() != rules.size() || !newRuleOrder.containsAll(rules.keySet())) {
            throw new IllegalArgumentException("New rule order does not account for all known rules or contains unknown rules.");
        }

        // create the new rule lookup - using a LinkedHashMap to perserve insertion order
        final Map<String, Rule> newRuleLookup = new LinkedHashMap<>();
        for (final String ruleId : newRuleOrder) {
            newRuleLookup.put(ruleId, rules.get(ruleId));
        }

        // save the new ordering
        rules = newRuleLookup;
    }

    /**
     * Returns a listing of all Rules.
     *
     * @return
     */
    public List<Rule> getRules() {
        return Collections.unmodifiableList(new ArrayList<>(rules.values()));
    }

    /**
     * Sets the flow file policy.
     *
     * @param flowFilePolicy
     */
    public void setFlowFilePolicy(FlowFilePolicy flowFilePolicy) {
        this.flowFilePolicy = flowFilePolicy;
    }

    /**
     * Gets the flow file policy.
     *
     * @return
     */
    public FlowFilePolicy getFlowFilePolicy() {
        return flowFilePolicy;
    }
}
