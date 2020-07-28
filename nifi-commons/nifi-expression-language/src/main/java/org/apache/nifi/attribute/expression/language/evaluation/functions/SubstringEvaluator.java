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
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class SubstringEvaluator extends StringEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<Long> startIndex;
    private final Evaluator<Long> endIndex;

    public SubstringEvaluator(final Evaluator<String> subject, final Evaluator<Long> startIndex, final Evaluator<Long> endIndex) {
        this.subject = subject;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public SubstringEvaluator(final Evaluator<String> subject, final Evaluator<Long> startIndex) {
        this.subject = subject;
        this.startIndex = startIndex;
        this.endIndex = null;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new StringQueryResult("");
        }
        final int startIndexValue = startIndex.evaluate(evaluationContext).getValue().intValue();
        if (endIndex == null) {
            return new StringQueryResult(subjectValue.substring(startIndexValue));
        } else {
            final int endIndexValue = endIndex.evaluate(evaluationContext).getValue().intValue();
            return new StringQueryResult(subjectValue.substring(startIndexValue, endIndexValue));
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
