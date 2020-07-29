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
package org.apache.nifi.attribute.expression.language.evaluation.selection;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.EvaluatorState;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

import java.util.ArrayList;
import java.util.List;

public class MultiNamedAttributeEvaluator extends MultiAttributeEvaluator {

    private final List<String> attributeNames;
    private final int evaluationType;

    public MultiNamedAttributeEvaluator(final List<String> attributeNames, final int evaluationType) {
        this.attributeNames = attributeNames;
        this.evaluationType = evaluationType;
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext evaluationContext) {
        State state = evaluationContext.getEvaluatorState().getState(this, State.class);
        if (state == null) {
            state = new State();
            evaluationContext.getEvaluatorState().putState(this, state);
        }
        state.matchingAttributeNames = new ArrayList<>(attributeNames);

        if (state.matchingAttributeNames.size() <= state.evaluationCount) {
            return new StringQueryResult(null);
        }

        return new StringQueryResult(evaluationContext.getExpressionValue(state.matchingAttributeNames.get(state.evaluationCount++)));
    }

    @Override
    public int getEvaluationsRemaining(final EvaluationContext context) {
        final EvaluatorState evaluatorState = context.getEvaluatorState();
        State state = evaluatorState.getState(this, State.class);
        if (state == null) {
            state = new State();
            evaluatorState.putState(this, state);
        }
        return state.matchingAttributeNames.size() - state.evaluationCount;
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

    @Override
    public int getEvaluationType() {
        return evaluationType;
    }

    @Override
    public Evaluator<?> getLogicEvaluator() {
        return this;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

    private class State {
        private int evaluationCount = 0;
        private List<String> matchingAttributeNames = null;
    }
}
