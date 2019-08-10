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
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;

abstract class PaddingEvaluator extends StringEvaluator {

    public static final String DEFAULT_PADDING_STRING = "_";

    private final Evaluator<String> subject;
    private final Evaluator<Long> desiredLength;
    private final Evaluator<String> pad;

    PaddingEvaluator(final Evaluator<String> subject, final Evaluator<Long> desiredLength, final Evaluator<String> pad) {
        this.subject = subject;
        this.desiredLength = desiredLength;
        this.pad = pad;
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new StringQueryResult(null);
        }
        final Long desiredLengthValue = desiredLength.evaluate(evaluationContext).getValue();
        if (desiredLengthValue == null || desiredLengthValue > Integer.MAX_VALUE || desiredLengthValue <= 0) {
            return new StringQueryResult(subjectValue);
        }

        String padValue = DEFAULT_PADDING_STRING;
        if (pad != null) {
            String s = pad.evaluate(evaluationContext).getValue();
            if (s != null && !s.isEmpty()) {
                padValue = s;
            }
        }

        return new StringQueryResult(doPad(subjectValue, desiredLengthValue.intValue(), padValue));
    }

    protected abstract String doPad(String subjectValue, int desiredLengthValue, String padValue);

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
