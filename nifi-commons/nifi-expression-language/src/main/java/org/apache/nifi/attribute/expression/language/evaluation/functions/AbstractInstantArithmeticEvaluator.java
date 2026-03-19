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
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.util.DateAmountParser;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Shared base for {@link PlusInstantDurationEvaluator} and {@link MinusInstantDurationEvaluator}.
 *
 * <p>Handles literal-argument validation at construction time and the common
 * evaluate-and-convert logic. Subclasses only provide the arithmetic direction
 * via {@link #applyAmount(ZonedDateTime, String)}.</p>
 */
abstract class AbstractInstantArithmeticEvaluator extends InstantEvaluator {

    private final Evaluator<Instant> subject;
    private final Evaluator<String> amountEvaluator;

    /**
     * @param subject         the instant-producing evaluator to operate on
     * @param amountEvaluator the evaluator producing the amount expression string
     */
    protected AbstractInstantArithmeticEvaluator(final Evaluator<Instant> subject,
                                                 final Evaluator<String> amountEvaluator) {
        this.subject = subject;
        this.amountEvaluator = amountEvaluator;
        if (amountEvaluator instanceof StringLiteralEvaluator) {
            DateAmountParser.validate(
                    ((StringLiteralEvaluator) amountEvaluator).evaluate(null).getValue());
        }
    }

    /** Apply the date arithmetic — plus or minus — to the given date-time. */
    protected abstract ZonedDateTime applyAmount(ZonedDateTime dateTime, String amountExpression);

    @Override
    public QueryResult<Instant> evaluate(final EvaluationContext evaluationContext) {
        final Instant subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new InstantQueryResult(null);
        }
        final String amountExpression = amountEvaluator.evaluate(evaluationContext).getValue();
        final ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(subjectValue, ZoneOffset.UTC);
        final ZonedDateTime result = applyAmount(zonedDateTime, amountExpression);
        return new InstantQueryResult(result.toInstant());
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
