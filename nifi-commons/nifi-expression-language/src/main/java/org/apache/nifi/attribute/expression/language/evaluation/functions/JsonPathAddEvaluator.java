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

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;

/**
 * JsonPathAddEvaluator allows adding a value to an array at the specified existing path
 */
public class JsonPathAddEvaluator extends JsonPathUpdateEvaluator {

    public JsonPathAddEvaluator(final Evaluator<String> subject, final Evaluator<String> jsonPathExp, final Evaluator<?> valueEvaluator) {
        super(subject, jsonPathExp, valueEvaluator);
    }

    @Override
    public DocumentContext updateAttribute(DocumentContext documentContext, JsonPath jsonPath, Object value) {
        return documentContext.add(jsonPath, value);
    }
}

