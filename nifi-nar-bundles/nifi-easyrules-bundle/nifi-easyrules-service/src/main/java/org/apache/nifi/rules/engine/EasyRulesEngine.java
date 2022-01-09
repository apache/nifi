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

import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.Rule;
import org.apache.nifi.rules.RulesMVELCondition;
import org.apache.nifi.rules.RulesSPELCondition;
import org.jeasy.rules.api.Condition;
import org.jeasy.rules.api.Fact;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.RuleListener;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.core.BasicRule;
import org.jeasy.rules.core.DefaultRulesEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class EasyRulesEngine implements RulesEngine {

    protected String rulesFileFormat;
    protected boolean ignoreConditionErrors;
    protected boolean filterRulesMissingFacts;
    protected Rules easyRules;
    protected List<RuleListener> ruleListeners;
    protected DefaultRulesEngine rulesEngine;


    public EasyRulesEngine(String rulesFileFormat, boolean ignoreConditionErrors, boolean filterRulesMissingFacts, List<Rule> rules) {
        this.rulesFileFormat = rulesFileFormat;
        this.ignoreConditionErrors = ignoreConditionErrors;
        this.filterRulesMissingFacts = filterRulesMissingFacts;
        this.easyRules = convertToEasyRules(rules, rulesFileFormat, ignoreConditionErrors);
        this.rulesEngine = new DefaultRulesEngine();
        if (getRuleListeners() != null) {
            rulesEngine.registerRuleListeners(getRuleListeners());
        }
    }

    /**
     * Return the list of actions what should be executed for a given set of facts
     *
     * @param facts a Map of key and facts values, as objects, that should be evaluated by the rules engine
     * @return
     */
    @Override
    public List<Action> fireRules(Map<String, Object> facts) {
        final List<Action> actions = new ArrayList<>();
        Map<Rule, Boolean> checkedRules = checkRules(facts);
        checkedRules.forEach((checkedRule, executeRule) -> {
            if (executeRule) {
                actions.addAll(checkedRule.getActions());
            }
        });
        return actions;
    }

    /**
     * Return a Map with Rule as a key and Boolean as a value indicating that the rule's conditions were met
     *
     * @param facts Map of keys and values contains facts to evaluate against rules
     * @return
     */
    @Override
    public Map<Rule, Boolean> checkRules(Map<String, Object> facts) {
        Map<Rule, Boolean> checkedRules = new HashMap<>();
        if (easyRules == null || facts == null || facts.isEmpty()) {
            return null;
        } else {
            Facts easyFacts = new Facts();
            facts.forEach(easyFacts::put);

            Map<org.jeasy.rules.api.Rule, Boolean> checkedEasyRules = rulesEngine.check(filterRulesMissingFacts ? filterByAvailableFacts(facts) : easyRules, easyFacts);
            checkedEasyRules.forEach((checkedRuled, executeAction) -> {
                checkedRules.put(((NiFiEasyRule) checkedRuled).getNifiRule(), executeAction);
            });

        }
        return checkedRules;
    }

    public List<Rule> getRules() {
        return StreamSupport.stream(easyRules.spliterator(), false)
                .map(easyRule -> ((NiFiEasyRule) easyRule)
                        .getNifiRule()).collect(Collectors.toList());
    }

    List<RuleListener> getRuleListeners() {
        return ruleListeners;
    }

    void setRuleListeners(List<RuleListener> ruleListeners) {
        this.ruleListeners = ruleListeners;
    }

    private org.jeasy.rules.api.Rules convertToEasyRules(List<Rule> rules, String rulesFileFormat, Boolean ignoreConditionErrors) {
        final Rules easyRules = new Rules();
        for (Rule rule : rules) {
            easyRules.register(new NiFiEasyRule(rule, rulesFileFormat, ignoreConditionErrors));
        }
        return easyRules;
    }

    private Rules filterByAvailableFacts(Map<String, Object> facts) {
        Set<String> factVariables = facts.keySet();
        List<org.jeasy.rules.api.Rule> filteredEasyRules = StreamSupport.stream(easyRules.spliterator(), false)
                .filter(easyRule -> ((NiFiEasyRule) easyRule).getNifiRule().getFacts() == null || factVariables.containsAll(((NiFiEasyRule) easyRule)
                        .getNifiRule().getFacts())).collect(Collectors.toList());

        return new Rules(new HashSet(filteredEasyRules));
    }

    private static class NiFiEasyRule extends BasicRule {

        private Condition condition;
        private Rule nifiRule;

        NiFiEasyRule(Rule nifiRule, String rulesFileFormat, Boolean ignoreConditionErrors) {
            super(nifiRule.getName(), nifiRule.getDescription(), nifiRule.getPriority());
            this.condition = rulesFileFormat.equalsIgnoreCase("spel")
                    ? new RulesSPELCondition(nifiRule.getCondition(), ignoreConditionErrors) : new RulesMVELCondition(nifiRule.getCondition(), ignoreConditionErrors);
            this.nifiRule = nifiRule;
        }

        public boolean evaluate(Facts facts) {

            final Facts evaluateFacts;

            if (nifiRule.getFacts() != null) {

                List<Fact<?>> filteredFacts = StreamSupport.stream(facts.spliterator(), false)
                        .filter(fact -> nifiRule.getFacts().contains(fact.getName())).collect(Collectors.toList());

                if (filteredFacts.size() > 0) {
                    evaluateFacts = new Facts();
                    filteredFacts.forEach(filteredFact -> {
                        evaluateFacts.put(filteredFact.getName(), filteredFact.getValue());
                    });
                } else {
                    evaluateFacts = facts;
                }

            } else {
                evaluateFacts = facts;
            }

            return this.condition.evaluate(evaluateFacts);

        }

        Rule getNifiRule() {
            return nifiRule;
        }
    }


}
