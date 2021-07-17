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

import com.jayway.jsonpath.PathNotFoundException;
import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JsonPathEvaluator provides access to document at the specified JsonPath
 */
public class JsonPathEvaluator extends JsonPathBaseEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathEvaluator.class);

    public JsonPathEvaluator(final Evaluator<String> subject, final Evaluator<String> jsonPathExp) {
        super(subject, jsonPathExp);
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext context) {
        DocumentContext documentContext = getDocumentContext(context);

        final JsonPath compiledJsonPath = getJsonPath(context);

        Object result = null;
        try {
            result = documentContext.read(compiledJsonPath);
        } catch (PathNotFoundException pnf) {
            // it is valid for a path not to be found, keys may not be there
            // do not spam the error log for this, instead we can log debug if enabled
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("PathNotFoundException for JsonPath " + compiledJsonPath.getPath(), pnf);
            }
            return EMPTY_RESULT;
        } catch (Exception e) {
            // a failure for something *other* than path not found however, should at least be
            // logged.
            LOGGER.error("Exception while reading JsonPath " + compiledJsonPath.getPath(), e);
            return EMPTY_RESULT;
        }

        return new StringQueryResult(getResultRepresentation(result, EMPTY_RESULT.getValue()));
    }

}

