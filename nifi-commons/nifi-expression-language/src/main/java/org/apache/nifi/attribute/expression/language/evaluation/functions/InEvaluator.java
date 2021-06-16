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
import org.apache.nifi.attribute.expression.language.evaluation.BooleanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;

import java.util.List;

public class InEvaluator extends BooleanEvaluator {

    private final Evaluator<String> subject;
    private final List<Evaluator<String>> search;

    public InEvaluator(final Evaluator<String> subject, final List<Evaluator<String>> list) {
        this.subject = subject;
        this.search = list;
    }

    @Override
    public QueryResult<Boolean> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new BooleanQueryResult(false);
        }

        boolean isInList = false;
        for (Evaluator<String> evaluator : search) {
            final String searchString = evaluator.evaluate(evaluationContext).getValue();
            isInList = searchString == null ? false : subjectValue.equals(searchString);
            if(isInList) {
                break;
            }
        }

        return new BooleanQueryResult(isInList);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
