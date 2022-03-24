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
package org.apache.nifi.attribute.expression.language.evaluation.reduce;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class JoinEvaluator extends StringEvaluator implements ReduceEvaluator<String> {

    private final Evaluator<String> subjectEvaluator;
    private final Evaluator<String> delimiterEvaluator;

    public JoinEvaluator(final Evaluator<String> subject, final Evaluator<String> delimiter) {
        this.subjectEvaluator = subject;
        this.delimiterEvaluator = delimiter;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        String subject = subjectEvaluator.evaluate(evaluationContext).getValue();
        if (subject == null) {
            subject = "";
        }

        final String delimiter = delimiterEvaluator.evaluate(evaluationContext).getValue();
        State state = evaluationContext.getEvaluatorState().getState(this, State.class);
        if (state == null) {
            state = new State();
            evaluationContext.getEvaluatorState().putState(this, state);
        }
        if (state.evalCount > 0) {
            state.sb.append(delimiter);
        }
        state.sb.append(subject);

        state.evalCount++;
        return new StringQueryResult(state.sb.toString());
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }

    private class State {
        private final StringBuilder sb = new StringBuilder();
        private int evalCount = 0;
    }
}
