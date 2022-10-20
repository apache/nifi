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
package org.apache.nifi.attribute.expression.language;

import org.apache.nifi.attribute.expression.language.evaluation.EvaluatorState;
import org.apache.nifi.parameter.Parameter;

import java.util.Objects;
import java.util.Set;

/**
 * Delegating implementation of Evaluation Context that blocks access to Sensitive Parameter Values
 */
public class NonSensitiveParameterEvaluationContext implements EvaluationContext {
    private final EvaluationContext evaluationContext;

    public NonSensitiveParameterEvaluationContext(final EvaluationContext evaluationContext) {
        this.evaluationContext = Objects.requireNonNull(evaluationContext, "Evaluation Context required");
    }

    @Override
    public String getExpressionValue(final String name) {
        return evaluationContext.getExpressionValue(name);
    }

    @Override
    public Set<String> getExpressionKeys() {
        return evaluationContext.getExpressionKeys();
    }

    @Override
    public String getState(final String key) {
        return evaluationContext.getState(key);
    }

    @Override
    public EvaluatorState getEvaluatorState() {
        return evaluationContext.getEvaluatorState();
    }

    /**
     * Get Parameter using Parameter Name and return when Parameter Descriptor is not sensitive
     *
     * @param parameterName Parameter Name
     * @return Non-Sensitive Parameter or null
     */
    @Override
    public Parameter getParameter(final String parameterName) {
        final Parameter parameter = evaluationContext.getParameter(parameterName);
        return (parameter == null || parameter.getDescriptor().isSensitive()) ? null : parameter;
    }
}
