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

public class DivideDecimalEvaluator extends DecimalEvaluator {

    private final Evaluator<Double> subject;
    private final Evaluator<Double> divideValue;

    public DivideDecimalEvaluator(final Evaluator<Double> subject, final Evaluator<Double> divideValue) {
        this.subject = subject;
        this.divideValue = divideValue;
    }

    @Override
    public QueryResult<Double> evaluate(final Map<String, String> attributes) {
        final Double subjectValue = subject.evaluate(attributes).getValue();
        if (subjectValue == null) {
            return new DecimalQueryResult(null);
        }

        final Double divide = divideValue.evaluate(attributes).getValue();
        if (divide == null) {
            return new DecimalQueryResult(null);
        }

        final Double result = subjectValue / divide;
        return new DecimalQueryResult(result);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
