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
import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JsonPathPutEvaluator allows setting or adding a key and scalar value at the specified existing path
 */
public class JsonPathPutEvaluator extends JsonPathUpdateEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathPutEvaluator.class);

    protected Evaluator<?> keyEvaluator;

    public JsonPathPutEvaluator(final Evaluator<String> subject, final Evaluator<String> jsonPathExp, final Evaluator<?> valueEvaluator, final Evaluator<?> keyEvaluator) {
        super(subject, jsonPathExp, valueEvaluator);
        this.keyEvaluator = keyEvaluator;
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext context) {
        DocumentContext documentContext = getDocumentContext(context);
        final JsonPath compiledJsonPath = getJsonPath(context);

        final Object value = valueEvaluator.evaluate(context).getValue();
        final String key = keyEvaluator.evaluate(context).getValue().toString();

        String result;
        try {
            result = documentContext.put(compiledJsonPath, key, value).jsonString();
        } catch (Exception e) {
            LOGGER.error("Failed to put value " + value + " at key " + key + " at path " + compiledJsonPath + " with error " + e.getLocalizedMessage(), e);
            // assume the path did not match anything in the document
            return EMPTY_RESULT;
        }

        return new StringQueryResult(getResultRepresentation(result, EMPTY_RESULT.getValue()));
    }

}

