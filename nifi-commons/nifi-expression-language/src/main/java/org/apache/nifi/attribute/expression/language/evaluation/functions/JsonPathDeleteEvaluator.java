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

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

/**
 * JsonPathDeleteEvaluator allows delete elements at the specified path
 */
public class JsonPathDeleteEvaluator extends JsonPathBaseEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathDeleteEvaluator.class);

    public JsonPathDeleteEvaluator(final Evaluator<String> subject, final Evaluator<String> jsonPathExp) {
        super(subject, jsonPathExp);
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext context) {
        DocumentContext documentContext = getDocumentContext(context);

        final JsonPath compiledJsonPath = getJsonPath(context);

        String result = null;
        try {
            result = documentContext.delete(compiledJsonPath).jsonString();
        } catch (PathNotFoundException pnf) {
            // it is valid for a path not to be found, keys may not be there
            // do not spam the error log for this, instead we can log debug if enabled
            LOGGER.debug("JSON Path not found: {}", compiledJsonPath.getPath(), pnf);
            result = documentContext.jsonString();
        } catch (Exception e) {
            // assume the path did not match anything in the document
            return EMPTY_RESULT;
        }

        return new StringQueryResult(getResultRepresentation(result, EMPTY_RESULT.getValue()));
    }

}

