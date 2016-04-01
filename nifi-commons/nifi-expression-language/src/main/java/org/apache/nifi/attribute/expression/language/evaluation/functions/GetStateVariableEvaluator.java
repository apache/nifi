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

import org.apache.nifi.attribute.expression.language.AttributesAndState;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

import java.util.Map;

public class GetStateVariableEvaluator extends StringEvaluator {

    private final Evaluator<String> subject;

    public GetStateVariableEvaluator(final Evaluator<String> subject) {
        this.subject = subject;
    }

    @Override
    public QueryResult<String> evaluate(Map<String, String> attributes) {
        if (!(attributes instanceof AttributesAndState)){
            return new StringQueryResult(null);
        }

        final String subjectValue = subject.evaluate(attributes).getValue();
        if (subjectValue == null) {
            return new StringQueryResult(null);
        }

        AttributesAndState attributesAndState = (AttributesAndState) attributes;

        Map<String, String> stateMap = attributesAndState.getStateMap();
        String stateValue = stateMap.get(subjectValue);

        return new StringQueryResult(stateValue);
    }

    @Override
    public Evaluator<String> getSubjectEvaluator() {
        return subject;
    }
}
