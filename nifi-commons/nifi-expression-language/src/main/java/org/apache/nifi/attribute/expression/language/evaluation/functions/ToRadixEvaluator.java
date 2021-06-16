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

import java.util.Arrays;

public class ToRadixEvaluator extends StringEvaluator {

    private final Evaluator<Long> numberEvaluator;
    private final Evaluator<Long> radixEvaluator;
    private final Evaluator<Long> minimumWidthEvaluator;

    public ToRadixEvaluator(final Evaluator<Long> subject, final Evaluator<Long> radixEvaluator) {
        this(subject, radixEvaluator, null);
    }

    public ToRadixEvaluator(final Evaluator<Long> subject, final Evaluator<Long> radixEvaluator, final Evaluator<Long> minimumWidthEvaluator) {
        this.numberEvaluator = subject;
        this.radixEvaluator = radixEvaluator;
        this.minimumWidthEvaluator = minimumWidthEvaluator;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final Long result = numberEvaluator.evaluate(evaluationContext).getValue();
        if (result == null) {
            return new StringQueryResult(null);
        }

        final Long radix = radixEvaluator.evaluate(evaluationContext).getValue();
        if (radix == null) {
            return new StringQueryResult(null);
        }

        String stringValue = Long.toString(result.longValue(), radix.intValue());
        if (minimumWidthEvaluator != null) {
            final Long minimumWidth = minimumWidthEvaluator.evaluate(evaluationContext).getValue();
            if (minimumWidth != null) {
                final int paddingWidth = minimumWidth.intValue() - stringValue.length();
                if (paddingWidth > 0) {
                    final char[] padChars = new char[paddingWidth];
                    Arrays.fill(padChars, '0');
                    stringValue = String.valueOf(padChars) + stringValue;
                }
            }
        }

        return new StringQueryResult(stringValue);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return numberEvaluator;
    }

}
