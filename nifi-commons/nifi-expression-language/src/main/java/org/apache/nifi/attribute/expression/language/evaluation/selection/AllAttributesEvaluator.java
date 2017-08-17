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

import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.BooleanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;

public class AllAttributesEvaluator extends BooleanEvaluator implements IteratingEvaluator<Boolean> {

    private final BooleanEvaluator booleanEvaluator;
    private final MultiAttributeEvaluator multiAttributeEvaluator;

    public AllAttributesEvaluator(final BooleanEvaluator booleanEvaluator, final MultiAttributeEvaluator multiAttributeEvaluator) {
        this.booleanEvaluator = booleanEvaluator;
        this.multiAttributeEvaluator = multiAttributeEvaluator;
    }

    @Override
    public QueryResult<Boolean> evaluate(final Map<String, String> attributes) {
        QueryResult<Boolean> attributeValueQuery = booleanEvaluator.evaluate(attributes);
        Boolean result = attributeValueQuery.getValue();
        if (result == null) {
            return new BooleanQueryResult(false);
        }

        if (!result) {
            return new BooleanQueryResult(false);
        }

        while (multiAttributeEvaluator.getEvaluationsRemaining() > 0) {
            attributeValueQuery = booleanEvaluator.evaluate(attributes);
            result = attributeValueQuery.getValue();
            if (result != null && !result) {
                return attributeValueQuery;
            }
        }

        return new BooleanQueryResult(true);
    }

    @Override
    public int getEvaluationsRemaining() {
        return 0;
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

    @Override
    public Evaluator<?> getLogicEvaluator() {
        return booleanEvaluator;
    }

    public MultiAttributeEvaluator getVariableIteratingEvaluator() {
        return multiAttributeEvaluator;
    }
}
