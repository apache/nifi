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
import java.util.regex.Pattern;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class MultiMatchAttributeEvaluator extends MultiAttributeEvaluator {

    private final List<Pattern> attributePatterns;
    private final int evaluationType;
    private final List<String> attributeNames = new ArrayList<>();
    private int evaluationCount = 0;

    public MultiMatchAttributeEvaluator(final List<String> attributeRegexes, final int evaluationType) {
        this.attributePatterns = new ArrayList<>();
        for (final String regex : attributeRegexes) {
            attributePatterns.add(Pattern.compile(regex));
        }

        this.evaluationType = evaluationType;
    }

    /**
     * Can be called only after the first call to evaluate
     *
     * @return number of remaining evaluations
     */
    @Override
    public int getEvaluationsRemaining() {
        return attributeNames.size() - evaluationCount;
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        if (evaluationCount == 0) {
            for (final Pattern pattern : attributePatterns) {
                for (final String attrName : attributes.keySet()) {
                    if (pattern.matcher(attrName).matches()) {
                        attributeNames.add(attrName);
                    }
                }
            }
        }

        if (evaluationCount >= attributeNames.size()) {
            return new StringQueryResult(null);
        }

        return new StringQueryResult(attributes.get(attributeNames.get(evaluationCount++)));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

    @Override
    public int getEvaluationType() {
        return evaluationType;
    }
}
