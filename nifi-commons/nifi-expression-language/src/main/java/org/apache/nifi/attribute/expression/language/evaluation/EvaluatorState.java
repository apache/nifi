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

import java.util.HashMap;
import java.util.Map;

/**
 * A storage mechanism for {@link Evaluator}s to stash & retrieve state. While most Evaluators are stateless,
 * some must maintain state, for instance to iterate over many Attributes. By stashing that state and retrieving
 * it from this EvaluatorState instead of using member variables, Evaluators are able to be kept both threadsafe
 * and in-and-of-themselves stateless, which means that new Evaluators need not be created for each iteration of
 * the function.
 */
public class EvaluatorState {

    private final Map<Evaluator<?>, Object> statePerEvaluator = new HashMap<>();

    /**
     * Fetches state for the given evaluator, casting it into the given type
     * @param evaluator the Evaluator to retrieve state for
     * @param clazz the Class to which the value should be cast
     * @param <T> type Type of the value
     * @return the state for the given Evaluator, or <code>null</code> if no state has been stored
     */
    public <T> T getState(Evaluator<?> evaluator, Class<T> clazz) {
        return clazz.cast(statePerEvaluator.get(evaluator));
    }

    /**
     * Updates state for the given Evaluator
     * @param evaluator the Evaluator to store state for
     * @param state the state to store
     */
    public void putState(Evaluator<?> evaluator, Object state) {
        statePerEvaluator.put(evaluator, state);
    }

}
