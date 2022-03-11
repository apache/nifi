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
import org.apache.nifi.attribute.expression.language.evaluation.InstantEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.InstantQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;

import java.time.Instant;

public class NumberToInstantEvaluator extends InstantEvaluator {

    private final Evaluator<Long> subject;

    public NumberToInstantEvaluator(final Evaluator<Long> subject) {
        this.subject = subject;
    }

    @Override
    public QueryResult<Instant> evaluate(final EvaluationContext evaluationContext) {
        final QueryResult<Long> result = subject.evaluate(evaluationContext);
        final Long value = result.getValue();
        if (value == null) {
            return new InstantQueryResult(null);
        }

        return new InstantQueryResult(Instant.ofEpochMilli(value));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }


}
