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

import org.jeasy.rules.api.Condition;
import org.jeasy.rules.api.Facts;
import org.mvel2.MVEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class RulesMVELCondition implements Condition {

    private static final Logger LOGGER = LoggerFactory.getLogger(RulesMVELCondition.class);
    private String expression;
    private Serializable compiledExpression;
    private boolean ignoreConditionErrors;

    public RulesMVELCondition(String expression, boolean ignoreConditionErrors) {
        this.expression = expression;
        this.compiledExpression = MVEL.compileExpression(expression);
        this.ignoreConditionErrors = ignoreConditionErrors;
    }

    public boolean evaluate(Facts facts) {
        try {
            return (Boolean)MVEL.executeExpression(this.compiledExpression, facts.asMap());
        } catch (Exception ex) {
            if(ignoreConditionErrors) {
                LOGGER.debug("Unable to evaluate expression: '" + this.expression + "' on facts: " + facts, ex);
                return false;
            } else{
                throw ex;
            }
        }
    }
}
