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
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;

public class PlusEvaluator extends NumberEvaluator {

    private final Evaluator<Number> subject;
    private final Evaluator<Number> plusValue;

    public PlusEvaluator(final Evaluator<Number> subject, final Evaluator<Number> plusValue) {
        this.subject = subject;
        this.plusValue = plusValue;
    }

    @Override
    public QueryResult<Number> evaluate(final EvaluationContext evaluationContext) {
        final Number subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new NumberQueryResult(null);
        }

        final Number plus = plusValue.evaluate(evaluationContext).getValue();
        if (plus == null) {
            return new NumberQueryResult(null);
        }

        final Number result;
        if (subjectValue instanceof Double || plus instanceof Double){
            result = subjectValue.doubleValue() + plus.doubleValue();
        } else {
            result = subjectValue.longValue() + plus.longValue();
        }
        return new NumberQueryResult(result);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
