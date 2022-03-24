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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JsonPathUpdateEvaluator is base class for updating attributes
 *
 * Subclasses need to implement {@link #updateAttribute} method otherwise it throws {@link NotImplementedException}
 */
public abstract class JsonPathUpdateEvaluator extends JsonPathBaseEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathUpdateEvaluator.class);

    protected Evaluator<?> valueEvaluator;

    public JsonPathUpdateEvaluator(final Evaluator<String> subject, final Evaluator<String> jsonPathExp, final Evaluator<?> valueEvaluator) {
        super(subject, jsonPathExp);
        this.valueEvaluator = valueEvaluator;
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext context) {
        DocumentContext documentContext = getDocumentContext(context);
        final JsonPath compiledJsonPath = getJsonPath(context);

        final Object value = valueEvaluator.evaluate(context).getValue();

        String result;
        try {
            result = updateAttribute(documentContext, compiledJsonPath, value).jsonString();
        } catch (Exception e) {
            LOGGER.error("Failed to update attribute " + e.getLocalizedMessage(), e);
            // assume the path did not match anything in the document
            return EMPTY_RESULT;
        }

        return new StringQueryResult(getResultRepresentation(result, EMPTY_RESULT.getValue()));
    }

    /**
     * Update the attribute at the specified path.  The subclasses will need to implement this method.
     * @param documentContext the document to be updated
     * @param jsonPath the path to update
     * @param value the value to be applied at the specified path
     * @return the updated DocumentContext
     * @throws NotImplementedException if operation is not implemented
     */
    public DocumentContext updateAttribute(DocumentContext documentContext, JsonPath jsonPath, Object value) {
        throw new NotImplementedException("Please implement updateAttribute method in the implementation class");
    }
}

