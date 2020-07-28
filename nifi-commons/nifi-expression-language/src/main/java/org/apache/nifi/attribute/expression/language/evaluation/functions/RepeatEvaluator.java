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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

public class RepeatEvaluator extends StringEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<Long> minRepeats;
    private final Evaluator<Long> maxRepeats;

    public RepeatEvaluator(final Evaluator<String> subject, final Evaluator<Long> minRepeats, final Evaluator<Long> maxRepeats) {
        this.subject = subject;
        this.minRepeats = minRepeats;
        this.maxRepeats = maxRepeats;
    }

    public RepeatEvaluator(final Evaluator<String> subject, final Evaluator<Long> minRepeats) {
        this.subject = subject;
        this.minRepeats = minRepeats;
        this.maxRepeats = null;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new StringQueryResult("");
        }
        final int firstRepeatValue = minRepeats.evaluate(evaluationContext).getValue().intValue();
        if (maxRepeats == null) {
            if (firstRepeatValue <= 0) {
                throw new AttributeExpressionLanguageException("Number of repeats must be > 0");
            }
            return new StringQueryResult(StringUtils.repeat(subjectValue, firstRepeatValue));
        } else {
            if (firstRepeatValue <= 0) {
                throw new AttributeExpressionLanguageException("Minimum number of repeats must be > 0");
            }
            final int maxRepeatCount = maxRepeats.evaluate(evaluationContext).getValue().intValue();
            if (firstRepeatValue > maxRepeatCount) {
                throw new AttributeExpressionLanguageException("Min repeats must not be greater than max repeats");
            }
            final int randomRepeatCount = ((int) (Math.random() * (maxRepeatCount - firstRepeatValue + 1))) + firstRepeatValue;
            return new StringQueryResult(StringUtils.repeat(subjectValue, randomRepeatCount));
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
