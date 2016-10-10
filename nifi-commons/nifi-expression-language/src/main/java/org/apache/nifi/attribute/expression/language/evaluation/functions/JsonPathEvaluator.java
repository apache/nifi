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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;


public class JsonPathEvaluator extends StringEvaluator {

    private static final StringQueryResult EMPTY_RESULT = new StringQueryResult("");
    private static final Configuration STRICT_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();
    private static final JsonProvider JSON_PROVIDER = STRICT_PROVIDER_CONFIGURATION.jsonProvider();

    private final Evaluator<String> subject;
    private final Evaluator<String> jsonPathExp;
    private final JsonPath precompiledJsonPathExp;

    public JsonPathEvaluator(final Evaluator<String> subject, final Evaluator<String> jsonPathExp) {
        this.subject = subject;
        this.jsonPathExp = jsonPathExp;
        // if the search string is a literal, we don't need to evaluate it each
        // time; we can just
        // pre-compile it. Otherwise, it must be compiled every time.
        if (jsonPathExp instanceof StringLiteralEvaluator) {
            precompiledJsonPathExp = compileJsonPathExpression(jsonPathExp.evaluate(null).getValue());
        } else {
            precompiledJsonPathExp = null;
        }

    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        final String subjectValue = subject.evaluate(attributes).getValue();
        if (subjectValue == null || subjectValue.length() == 0) {
            throw new  AttributeExpressionLanguageException("Subject is empty");
        }
        DocumentContext documentContext = null;
        try {
            documentContext = validateAndEstablishJsonContext(subjectValue);
        } catch (InvalidJsonException e) {
            throw new AttributeExpressionLanguageException("Subject contains invalid JSON: " + subjectValue, e);
        }

        final JsonPath compiledJsonPath;
        if (precompiledJsonPathExp != null) {
            compiledJsonPath = precompiledJsonPathExp;
        } else {
            compiledJsonPath = compileJsonPathExpression(jsonPathExp.evaluate(attributes).getValue());
        }

        Object result = null;
        try {
            result = documentContext.read(compiledJsonPath);
        } catch (Exception e) {
            // assume the path did not match anything in the document
            return EMPTY_RESULT;
        }

        return new StringQueryResult(getResultRepresentation(result, EMPTY_RESULT.getValue()));
    }


    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

    static DocumentContext validateAndEstablishJsonContext(final String json) {
        final DocumentContext ctx = JsonPath.using(STRICT_PROVIDER_CONFIGURATION).parse(json);
        return ctx;
    }

    static boolean isJsonScalar(final Object obj) {
        return !(obj instanceof Map || obj instanceof List);
    }

    static String getResultRepresentation(final Object jsonPathResult, final String defaultValue) {
        if (isJsonScalar(jsonPathResult)) {
            return Objects.toString(jsonPathResult, defaultValue);
        } else if (jsonPathResult instanceof List && ((List<?>) jsonPathResult).size() == 1) {
            return getResultRepresentation(((List<?>) jsonPathResult).get(0), defaultValue);
        } else {
            return JSON_PROVIDER.toJson(jsonPathResult);
        }
    }

    static JsonPath compileJsonPathExpression(String exp) {
        try {
            return JsonPath.compile(exp);
        } catch (Exception e) {
            throw new AttributeExpressionLanguageException("Invalid JSON Path expression: " + exp, e);
        }
    }

}

