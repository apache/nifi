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
package org.apache.nifi.attribute.expression.language.evaluation.selection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class MultiNamedAttributeEvaluator extends MultiAttributeEvaluator {

    private final List<String> attributeNames;
    private final int evaluationType;
    private int evaluationCount = 0;
    private List<String> matchingAttributeNames = null;

    public MultiNamedAttributeEvaluator(final List<String> attributeNames, final int evaluationType) {
        this.attributeNames = attributeNames;
        this.evaluationType = evaluationType;
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        matchingAttributeNames = new ArrayList<>(attributeNames);

        if (matchingAttributeNames.size() <= evaluationCount) {
            return new StringQueryResult(null);
        }

        return new StringQueryResult(attributes.get(matchingAttributeNames.get(evaluationCount++)));
    }

    @Override
    public int getEvaluationsRemaining() {
        return matchingAttributeNames.size() - evaluationCount;
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

    @Override
    public int getEvaluationType() {
        return evaluationType;
    }

    @Override
    public Evaluator<?> getLogicEvaluator() {
        return this;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }
}
