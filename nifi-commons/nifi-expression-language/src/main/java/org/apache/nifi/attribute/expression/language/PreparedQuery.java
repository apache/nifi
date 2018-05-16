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


import java.util.Map;

import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.processor.exception.ProcessException;

public interface PreparedQuery {

    String evaluateExpressions(Map<String, String> valueLookup, AttributeValueDecorator decorator) throws ProcessException;

    String evaluateExpressions(final Map<String, String> valueLookup, final AttributeValueDecorator decorator, final Map<String, String> stateVariables) throws ProcessException;

    boolean isExpressionLanguagePresent();

    /**
     * Returns a {@link VariableImpact} that can be used to determine whether or not a given
     * variable impacts this Expression.
     *
     * @return a {@link VariableImpact} that can be used to determine whether or not a given
     *         variable impacts this Expression.
     */
    VariableImpact getVariableImpact();
}
