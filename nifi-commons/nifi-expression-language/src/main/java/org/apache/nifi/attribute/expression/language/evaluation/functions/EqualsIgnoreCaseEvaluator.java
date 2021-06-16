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
package org.apache.nifi.attribute.expression.language.evaluation.functions;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;

public class EqualsIgnoreCaseEvaluator extends BooleanEvaluator {

    private final Evaluator<?> subject;
    private final Evaluator<?> compareTo;

    public EqualsIgnoreCaseEvaluator(final Evaluator<?> subject, final Evaluator<?> compareTo) {
        this.subject = subject;
        this.compareTo = compareTo;
    }

    @Override
    public QueryResult<Boolean> evaluate(final EvaluationContext evaluationContext) {
        final Object a = subject.evaluate(evaluationContext).getValue();
        if (a == null) {
            return new BooleanQueryResult(false);
        }

        final Object b = compareTo.evaluate(evaluationContext).getValue();
        if (b == null) {
            return new BooleanQueryResult(false);
        }

        if (a instanceof String && b instanceof String) {
            return new BooleanQueryResult(((String) a).equalsIgnoreCase((String) b));
        }

        return new BooleanQueryResult(a.equals(b));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
