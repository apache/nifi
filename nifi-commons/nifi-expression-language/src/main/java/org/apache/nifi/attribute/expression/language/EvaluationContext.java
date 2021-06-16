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

import java.util.Set;

public interface EvaluationContext {
    /**
     * Returns the name of an attribute, variable, environment variable, or system variable that can be referenced in the Expression Language
     * @param name the name of the attribute, variable, etc.
     * @return the value assigned to the attribute, variable, etc. or <code>null</code> if no such value exists
     */
    String getExpressionValue(String name);

    /**
     * Returns the names of all attributes, variables, etc. that can be used in Expression Language
     * @return the names of all keys that can be used in Expression Language
     */
    Set<String> getExpressionKeys();

    String getState(String key);

    Parameter getParameter(String parameterName);

    EvaluatorState getEvaluatorState();
}
