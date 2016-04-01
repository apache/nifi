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

import org.apache.nifi.attribute.expression.language.evaluation.DecimalEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.DecimalQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;

import java.util.Map;

public class MinusDecimalEvaluator extends DecimalEvaluator {

    private final Evaluator<Double> subject;
    private final Evaluator<Double> minusValue;

    public MinusDecimalEvaluator(final Evaluator<Double> subject, final Evaluator<Double> minusValue) {
        this.subject = subject;
        this.minusValue = minusValue;
    }

    @Override
    public QueryResult<Double> evaluate(final Map<String, String> attributes) {
        final Double subjectValue = subject.evaluate(attributes).getValue();
        if (subjectValue == null) {
            return new DecimalQueryResult(null);
        }

        final Double minus = minusValue.evaluate(attributes).getValue();
        if (minus == null) {
            return new DecimalQueryResult(null);
        }

        final double result = subjectValue - minus;
        return new DecimalQueryResult(result);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
