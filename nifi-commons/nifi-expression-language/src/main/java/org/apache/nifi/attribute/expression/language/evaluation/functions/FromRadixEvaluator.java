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
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberQueryResult;

import java.util.Map;

public class FromRadixEvaluator extends WholeNumberEvaluator {

    private final Evaluator<String> numberEvaluator;
    private final Evaluator<Long> radixEvaluator;

    public FromRadixEvaluator(final Evaluator<String> subject, final Evaluator<Long> radixEvaluator) {
        this.numberEvaluator = subject;
        this.radixEvaluator = radixEvaluator;
    }

    @Override
    public QueryResult<Long> evaluate(final Map<String, String> attributes) {
        final String result = numberEvaluator.evaluate(attributes).getValue();
        if (result == null) {
            return new WholeNumberQueryResult(null);
        }

        final Long radix = radixEvaluator.evaluate(attributes).getValue();
        if (radix == null) {
            return new WholeNumberQueryResult(null);
        }

        long longValue = Long.parseLong(result, radix.intValue());

        return new WholeNumberQueryResult(longValue);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return numberEvaluator;
    }

}
