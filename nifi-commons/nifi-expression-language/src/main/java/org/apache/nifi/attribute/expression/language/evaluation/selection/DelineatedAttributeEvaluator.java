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

import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class DelineatedAttributeEvaluator extends MultiAttributeEvaluator {

    private final Evaluator<String> subjectEvaluator;
    private final Evaluator<String> delimiterEvaluator;
    private final int evaluationType;
    private String[] delineatedValues;
    private int evaluationCount = 0;
    private int evaluationsLeft = 1;

    public DelineatedAttributeEvaluator(final Evaluator<String> subjectEvaluator, final Evaluator<String> delimiterEvaluator, final int evaluationType) {
        this.subjectEvaluator = subjectEvaluator;
        this.delimiterEvaluator = delimiterEvaluator;
        this.evaluationType = evaluationType;
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        if (delineatedValues == null) {
            final QueryResult<String> subjectValue = subjectEvaluator.evaluate(attributes);
            if (subjectValue.getValue() == null) {
                evaluationsLeft = 0;
                return new StringQueryResult(null);
            }

            final QueryResult<String> delimiterValue = delimiterEvaluator.evaluate(attributes);
            if (subjectValue.getValue() == null) {
                evaluationsLeft = 0;
                return new StringQueryResult(null);
            }

            delineatedValues = subjectValue.getValue().split(delimiterValue.getValue());
        }

        if (evaluationCount > delineatedValues.length || delineatedValues.length == 0) {
            evaluationsLeft = 0;
            return new StringQueryResult(null);
        }

        evaluationsLeft = delineatedValues.length - evaluationCount - 1;

        return new StringQueryResult(delineatedValues[evaluationCount++]);
    }

    @Override
    public Evaluator<?> getLogicEvaluator() {
        return subjectEvaluator;
    }

    @Override
    public int getEvaluationsRemaining() {
        return evaluationsLeft;
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

    @Override
    public int getEvaluationType() {
        return evaluationType;
    }
}
