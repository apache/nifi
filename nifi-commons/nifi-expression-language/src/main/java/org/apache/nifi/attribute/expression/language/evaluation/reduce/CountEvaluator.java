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
package org.apache.nifi.attribute.expression.language.evaluation.reduce;

import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberQueryResult;
import org.apache.nifi.expression.AttributeExpression.ResultType;

public class CountEvaluator extends WholeNumberEvaluator implements ReduceEvaluator<Long> {

    private final Evaluator<?> subjectEvaluator;
    private long count = 0L;

    public CountEvaluator(final Evaluator<?> subjectEvaluator) {
        this.subjectEvaluator = subjectEvaluator;
    }

    @Override
    public QueryResult<Long> evaluate(final Map<String, String> attributes) {
        final QueryResult<?> result = subjectEvaluator.evaluate(attributes);
        if (result.getValue() == null) {
            return new WholeNumberQueryResult(count);
        }

        if (result.getResultType() == ResultType.BOOLEAN && ((Boolean) result.getValue()).equals(Boolean.FALSE)) {
            return new WholeNumberQueryResult(count);
        }

        count++;
        return new WholeNumberQueryResult(count);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }

}
