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
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.registry.VariableRegistry;

import java.util.Map;
import java.util.Set;

public class StandardEvaluationContext implements EvaluationContext {
    private final ValueLookup valueLookup;
    private final Map<String, String> stateMap;
    private final ParameterLookup parameterLookup;
    private final EvaluatorState evaluatorState = new EvaluatorState();

    public StandardEvaluationContext(final Map<String, String> variables) {
        this(variables, null, ParameterLookup.EMPTY);
    }

    public StandardEvaluationContext(final Map<String, String> variables, final Map<String, String> stateMap, final ParameterLookup parameterLookup) {
        this(new ValueLookup(VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY, null, variables), stateMap, parameterLookup);
    }

    public StandardEvaluationContext(final ValueLookup valueLookup, final Map<String, String> stateMap, final ParameterLookup parameterLookup) {
        this.valueLookup = valueLookup;
        this.stateMap = stateMap;
        this.parameterLookup = parameterLookup;
    }

    @Override
    public String getExpressionValue(final String name) {
        return valueLookup.get(name);
    }

    @Override
    public Set<String> getExpressionKeys() {
        return valueLookup.getKeysAddressableByMultiMatch();
    }

    @Override
    public String getState(final String key) {
        return stateMap.get(key);
    }

    @Override
    public Parameter getParameter(final String parameterName) {
        return parameterLookup.getParameter(parameterName).orElse(null);
    }

    @Override
    public EvaluatorState getEvaluatorState() {
        return evaluatorState;
    }
}
