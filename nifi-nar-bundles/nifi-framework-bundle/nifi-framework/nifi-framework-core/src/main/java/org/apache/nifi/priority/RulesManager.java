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
package org.apache.nifi.priority;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.StandardEvaluationContext;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// This class will be used to manage the global ruleset for use with the GlobalPriorityFlowFileQueue.
public class RulesManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(RulesManager.class);

    private volatile List<Rule> ruleList = Collections.synchronizedList(new ArrayList<>());
    private final File configFile;
    private static final Gson GSON = new Gson();
    private static final JsonParser JSON_PARSER = new JsonParser();
    private static final String UUID = "uuid";
    private static final String EXPRESSION = "expression";
    private static final String RATE_OF_THREAD_USAGE = "rateOfThreadUsage";
    private static final String LABEL = "label";
    private static final String EXPIRED = "expired";
    private final ReentrantReadWriteLock.WriteLock writeLock = new ReentrantReadWriteLock(true).writeLock();
    private final StateManagerProvider stateManagerProvider;
    private final String stateId = "RulesManagerID";
    private final Scope stateScope = Scope.CLUSTER;
    private ScheduledExecutorService executorService;
    private static final long STATE_CHECK_FREQUENCY = TimeUnit.SECONDS.toMillis(1);
    private volatile boolean useState;
    private volatile Map<String, String> lastStateMapPolled;

    public RulesManager(File configFile, StateManagerProvider stateManagerProvider) {
        addRules(Arrays.asList(Rule.DEFAULT, Rule.UNEVALUATED));
        this.configFile = configFile;
        this.stateManagerProvider = stateManagerProvider;
        useState = stateManagerProvider != null;
    }

    public void readRules() {
        if(useState) {
            if(executorService == null) {
                executorService = Executors.newSingleThreadScheduledExecutor();
                executorService.scheduleAtFixedRate(this::readRulesFromStateManagerProvider, 0, STATE_CHECK_FREQUENCY, TimeUnit.MILLISECONDS);
            } else {
                // Assume we're already monitoring rule changes so by calling this method we want to force a re-read of what is there
                readRulesFromStateManagerProvider();
            }
        } else if(configFile != null && configFile.exists() && configFile.canRead()) {
            readRulesFromConfigFile();
        }
    }

    private void readRulesFromStateManagerProvider() {
        LOGGER.trace("readRulesFromStateManagerProvider");

        try {
            Map<String, String> ruleSet = stateManagerProvider.getStateManager(stateId).getState(stateScope).toMap();
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Received the following rules from state: {}", GSON.toJson(ruleList));
            }

            // No need to update our ruleset if the state hasn't changed
            if(!ruleSet.equals(lastStateMapPolled)) {
                ruleSet.values().stream().map(this::convertJsonStringToRule).filter(Objects::nonNull).forEach(this::addOrModifyRule);
                LOGGER.debug("Loaded {} rules to add or update.", ruleSet.size());
                lastStateMapPolled = ruleSet;
            }
        } catch (Exception e) {
            LOGGER.error("Exception occurred when trying to update rules from the stateManagerProvider.", e);
        }
    }

    private void readRulesFromConfigFile() {
        try {
            String configString = FileUtils.readFileToString(configFile, "UTF-8");
            JsonArray jsonArray = GSON.fromJson(configString, JsonArray.class).getAsJsonArray();
            List<Rule> ruleList = new ArrayList<>(jsonArray.size());
            jsonArray.forEach(jsonElement -> {
                if(jsonElement.isJsonObject()) {
                    JsonObject jsonObject = jsonElement.getAsJsonObject();
                    String uuid = jsonObject.has(UUID) ?  jsonObject.get(UUID).getAsString() : null;
                    String expression = jsonObject.has(EXPRESSION) ? jsonObject.get(EXPRESSION).getAsString() : null;
                    String label = jsonObject.has(LABEL) ? jsonObject.get(LABEL).getAsString() : "";
                    int rateOfThreadUsage = jsonObject.has(RATE_OF_THREAD_USAGE) ? jsonObject.get(RATE_OF_THREAD_USAGE).getAsInt() : Rule.DEFAULT_RATE_OF_THREAD_USAGE;
                    try {
                        ruleList.add(Rule.builder()
                                .uuid(uuid)
                                .expression(expression)
                                .label(label)
                                .rateOfThreadUsage(rateOfThreadUsage)
                                .build());
                    } catch (IllegalArgumentException e) {
                        LOGGER.warn("Unable to create rule using the following due to an exception:\n"
                                + "uuid: {}\n"
                                + "label: {}\n"
                                + "expression: {}\n"
                                + "rateOfThreadUsage: {}", uuid, label, expression, rateOfThreadUsage, e);
                    }
                }
            });

            LOGGER.debug("Loaded {} rules into memory.", ruleList.size());

            addRules(ruleList);
        } catch (Exception e) {
            LOGGER.error("Exception while trying to load ruleset. Any attempt to updates rules in the system may overwrite "
                            + "the existing rules on disk at {}. If this is not desired, stop the system and address the error.",
                    configFile.getAbsolutePath(), e);
        }
    }

    public void purgeExpiredRulesFromState() {
        if(useState) {
            try {
                Map<String, String> rulesFromStateManagement = new HashMap<>(stateManagerProvider.getStateManager(stateId).getState(stateScope).toMap());
                List<String> expiredRuleList = rulesFromStateManagement.values().stream()
                        .map(this::convertJsonStringToRule)
                        .filter(Objects::nonNull)
                        .filter(Rule::isExpired)
                        .map(Rule::getUuid)
                        .collect(Collectors.toList());
                expiredRuleList.forEach(rulesFromStateManagement::remove);
                stateManagerProvider.getStateManager(stateId).setState(rulesFromStateManagement, stateScope);
            } catch(Exception e) {
                LOGGER.warn("Exception occurred when attempting to purge expired rules from state management.", e);
            }
        }
    }

    Rule convertJsonStringToRule(String ruleString) {
        try {
            JsonObject jsonObject = JSON_PARSER.parse(ruleString).getAsJsonObject();
            boolean expired = jsonObject.has(EXPIRED) && jsonObject.get(EXPIRED).getAsBoolean();
            String uuid = jsonObject.has(UUID) ? jsonObject.get(UUID).getAsString() : null;
            String expression = jsonObject.has((EXPRESSION)) ? jsonObject.get(EXPRESSION).getAsString() : null;
            String label = jsonObject.has(LABEL) ? jsonObject.get(LABEL).getAsString() : "";
            int rateOfThreadUsage = jsonObject.has(RATE_OF_THREAD_USAGE) ? jsonObject.get(RATE_OF_THREAD_USAGE).getAsInt() : Rule.DEFAULT_RATE_OF_THREAD_USAGE;

            Rule newRule = Rule.builder()
                    .uuid(uuid)
                    .expression(expression)
                    .expired(expired)
                    .label(label)
                    .rateOfThreadUsage(rateOfThreadUsage)
                    .build();

            LOGGER.debug("convertJsonStringToRule(): Parsed Rule {} from String {}", newRule, ruleString);

            return newRule;
        } catch(Exception e) {
            LOGGER.warn("Cannot convert \"{}\" to a Rule due to exception.", ruleString, e);
            return null;
        }
    }

    String convertRuleToJsonString(Rule rule) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(UUID, rule.getUuid());
        jsonObject.addProperty(EXPRESSION, rule.getExpression());
        jsonObject.addProperty(LABEL, rule.getLabel());
        jsonObject.addProperty(RATE_OF_THREAD_USAGE, rule.getRateOfThreadUsage());
        jsonObject.addProperty(EXPIRED, rule.isExpired());
        return GSON.toJson(jsonObject);
    }

    public void writeRules() {
        if(useState) {
            writeRulesToState();
        } else {
            writeRulesToConfigFile();
        }
    }

    private void writeRulesToState() {
        try {
            writeLock.lock();
            Map<String, String> ruleMap = ruleList.stream().filter(rule -> rule != Rule.UNEVALUATED && rule != Rule.DEFAULT)
                    .collect(Collectors.toMap(Rule::getUuid, this::convertRuleToJsonString));
            stateManagerProvider.getStateManager(stateId).setState(ruleMap, stateScope);
        } catch (IOException e) {
            LOGGER.error("Exception when writing rules to state manager: ", e);
        } finally {
            writeLock.unlock();
        }
    }

    private void writeRulesToConfigFile() {
        if(configFile != null) {
            writeLock.lock();
            try {
                JsonArray jsonArray = new JsonArray(ruleList.size());
                ruleList.stream().filter(rule -> (rule != Rule.DEFAULT && rule != Rule.UNEVALUATED && !rule.isExpired()))
                        .forEach(rule -> {
                            JsonObject jsonObject = new JsonObject();
                            jsonObject.addProperty(UUID, rule.getUuid());
                            jsonObject.addProperty(EXPRESSION, rule.getExpression());
                            jsonObject.addProperty(RATE_OF_THREAD_USAGE, rule.getRateOfThreadUsage());
                            jsonObject.addProperty(LABEL, rule.getLabel());
                            jsonArray.add(jsonObject);
                        });

                FileUtils.write(configFile, jsonArray.toString(), "UTF-8");
            } catch (IOException e) {
                LOGGER.error("Exception when writing rules to config file:", e);
            } finally {
                writeLock.unlock();
            }
        }
    }

    public List<Rule> getRuleList() {
        return Collections.unmodifiableList(ruleList);
    }

    public List<Rule> getActiveRuleList() {
        return ruleList.stream().filter(rule -> !rule.isExpired()).collect(Collectors.toList());
    }

    public void addOrModifyRule(Rule targetRule) {
        if(ruleList.stream().noneMatch(targetRule::equals)) {
            addRule(targetRule);
        } else {
            modifyRule(targetRule);
        }
    }

    public void addRule(Rule newRule) {
        writeLock.lock();
        try {
            ruleList.add(newRule);
            ruleList.sort(Comparator.naturalOrder());
        } finally {
            writeLock.unlock();
        }
    }

    public void addRules(List<Rule> newRules) {
        writeLock.lock();
        try {
            ruleList.addAll(newRules);
            ruleList.sort(Comparator.naturalOrder());
        } finally {
            writeLock.unlock();
        }
    }

    public void modifyRule(Rule modifiedRule) {
        writeLock.lock();
        try {
            Optional<Rule> targetRuleOptional = ruleList.stream().filter(modifiedRule::equals).findFirst();
            if(!targetRuleOptional.isPresent()) {
                // Nothing to modify, no-op
                return;
            }

            Rule targetRule = targetRuleOptional.get();

            // We are not changing the expression so we may modify the existing rule versus replacing it
            if(targetRule.getExpression().equals(modifiedRule.getExpression())) {
                ruleList.stream().filter(modifiedRule::equals).findAny().ifPresent(rule ->
                        rule.setLabel(modifiedRule.getLabel()).setRateOfThreadUsage(modifiedRule.getRateOfThreadUsage()).setExpired(modifiedRule.isExpired()));
            } else {
                // Because we are modifying the expression what we really need is a new rule as any flowfiles already associated with the existing
                // rule now need to be re-evaluated
                Rule newRule = Rule.builder()
                        .expression(modifiedRule.getExpression())
                        .rateOfThreadUsage(modifiedRule.getRateOfThreadUsage())
                        .label(modifiedRule.getLabel())
                        .expired(modifiedRule.isExpired())
                        .build();
                targetRule.setExpired(true);
                ruleList.add(newRule);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Rule getRule(FlowFileRecord flowFileRecord) {
        for(Rule rule: ruleList) {
            if(rule != Rule.DEFAULT && rule != Rule.UNEVALUATED && !rule.isExpired()) {
                try {
                    final EvaluationContext evaluationContext = new StandardEvaluationContext(flowFileRecord.getAttributes());

                    final String evaluated = rule.getQuery().evaluateExpressions(evaluationContext, null);
                    if(Boolean.parseBoolean(evaluated)) {
                        return rule;
                    }
                } catch(ProcessException e) {
                    LOGGER.warn("Exception while evaluating expression for rule {}", rule, e);
                }
            }
        }

        return Rule.DEFAULT;
    }
}
