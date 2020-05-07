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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class RulesSPELCondition implements Condition {
    private static final Logger LOGGER = LoggerFactory.getLogger(RulesSPELCondition.class);
    private final ExpressionParser parser = new SpelExpressionParser();
    private String expression;
    private Expression compiledExpression;
    private boolean ignoreConditionErrors;


    public RulesSPELCondition(String expression) {
        this.expression = expression;
        this.compiledExpression = this.parser.parseExpression(expression);
    }

    public RulesSPELCondition(String expression, ParserContext parserContext) {
        this.expression = expression;
        this.compiledExpression = this.parser.parseExpression(expression, parserContext);
    }

    public RulesSPELCondition(String expression, boolean ignoreConditionErrors) {
        this.expression = expression;
        this.compiledExpression =  this.parser.parseExpression(expression);
        this.ignoreConditionErrors = ignoreConditionErrors;
    }

    public RulesSPELCondition(String expression, ParserContext parserContext, boolean ignoreConditionErrors) {
        this.expression = expression;
        this.compiledExpression =  this.parser.parseExpression(expression, parserContext);
        this.ignoreConditionErrors = ignoreConditionErrors;
    }

    public boolean evaluate(Facts facts) {
        try {
            StandardEvaluationContext context = new StandardEvaluationContext();
            context.setRootObject(facts.asMap());
            context.setVariables(facts.asMap());
            return this.compiledExpression.getValue(context, Boolean.class);
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
