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
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

import java.util.Map;

public class IfElseEvaluator extends StringEvaluator {

    private final Evaluator<Boolean> subject;
    private final Evaluator<String> trueEvaluator;
    private final Evaluator<String> falseEvaluator;

    public IfElseEvaluator(final Evaluator<Boolean> subject, final Evaluator<String> trueEvaluator, final Evaluator<String> falseEvaluator) {
        this.subject = subject;
        this.trueEvaluator = trueEvaluator;
        this.falseEvaluator = falseEvaluator;
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        final QueryResult<Boolean> subjectValue = subject.evaluate(attributes);
        if (subjectValue == null) {
            return new StringQueryResult(null);
        }
        final String ifElseValue = (Boolean.TRUE.equals(subjectValue.getValue())) ? trueEvaluator.evaluate(attributes).getValue() : falseEvaluator.evaluate(attributes).getValue();
        return new StringQueryResult(ifElseValue);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
