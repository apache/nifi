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

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.util.DateAmountParser;

import java.time.ZonedDateTime;
import java.util.Date;

/**
 * Evaluator for {@code plusDuration('3 months')} — adds a calendar-aware amount to a Date.
 *
 * <p>Examples:</p>
 * <pre>
 *   ${date:toDate('dd-MM-yyyy'):plusDuration('1 week'):format('dd-MM-yyyy')}
 *   ${date:toDate('dd-MM-yyyy'):plusDuration('2 years')}
 * </pre>
 */
public class PlusDurationEvaluator extends AbstractDateArithmeticEvaluator {

    public PlusDurationEvaluator(final Evaluator<Date> subject, final Evaluator<String> amountEvaluator) {
        super(subject, amountEvaluator);
    }

    @Override
    protected ZonedDateTime applyAmount(final ZonedDateTime dateTime, final String amountExpression) {
        return DateAmountParser.plus(dateTime, amountExpression);
    }
}
