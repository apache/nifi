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
package org.apache.nifi.attribute.expression.language.evaluation;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.expression.AttributeExpression.ResultType;

public interface Evaluator<T> {

    QueryResult<T> evaluate(EvaluationContext evaluationContext);

    ResultType getResultType();

    int getEvaluationsRemaining(EvaluationContext context);

    Evaluator<?> getSubjectEvaluator();

    /**
     * Sets the token that was used in the query to cause this Evaluator to be created
     *
     * @param token the token that caused this Evaluator to be created
     */
    void setToken(String token);

    /**
     * @return the token that caused this Evaluator to be created
     */
    String getToken();
}
