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
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.RuleListener;


import java.util.ArrayList;
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
public class EasyRulesEngineService  extends EasyRulesEngineProvider implements RulesEngineService {

    private volatile RulesEngine rulesEngine;

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        EasyRulesEngine easyRulesEngine = (EasyRulesEngine) getRulesEngine();
        List<RuleListener> ruleListeners = new ArrayList<>();
        ruleListeners.add(new EasyRulesListener(getLogger()));
        easyRulesEngine.setRuleListeners(ruleListeners);
        rulesEngine = easyRulesEngine;
    }

    /**
     * Return the list of actions what should be executed for a given set of facts
     * @param facts a Map of key and facts values, as objects, that should be evaluated by the rules engine
     * @return List of Actions
     */
    @Override
    public List<Action> fireRules(Map<String, Object> facts) {
        return rulesEngine.fireRules(facts);
    }

    private static class EasyRulesListener implements RuleListener {

        private ComponentLog logger;

        EasyRulesListener(ComponentLog logger) {
            this.logger = logger;
        }

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
            logger.debug("Rules was successfully processed for: {}",new Object[]{rule.getName()});
        }

        @Override
        public void onFailure(org.jeasy.rules.api.Rule rule, Facts facts, Exception e) {
            logger.warn("Rule execution failed for: {}", rule.getName(), e);
        }
    }


}
