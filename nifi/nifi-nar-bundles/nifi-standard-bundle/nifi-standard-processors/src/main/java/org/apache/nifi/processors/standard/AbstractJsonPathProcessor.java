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
package org.apache.nifi.processors.standard;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.internal.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import net.minidev.json.parser.JSONParser;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.standard.util.JsonPathExpressionValidator;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.ObjectHolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Provides common functionality used for processors interacting and manipulating JSON data via JsonPath.
 *
 * @see <a href="http://json.org">http://json.org</a>
 * @see <a href="https://github.com/jayway/JsonPath">https://github.com/jayway/JsonPath</a>
 */
public abstract class AbstractJsonPathProcessor extends AbstractProcessor {

    private static final Configuration STRICT_PROVIDER_CONFIGURATION =
            Configuration.builder().jsonProvider(new JsonSmartJsonProvider(JSONParser.MODE_RFC4627)).build();

    private static final JsonProvider JSON_PROVIDER = STRICT_PROVIDER_CONFIGURATION.jsonProvider();

    static DocumentContext validateAndEstablishJsonContext(ProcessSession processSession, FlowFile flowFile) {
        // Parse the document once into an associated context to support multiple path evaluations if specified
        final ObjectHolder<DocumentContext> contextHolder = new ObjectHolder<>(null);
        processSession.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try (BufferedInputStream bufferedInputStream = new BufferedInputStream(in)) {
                    DocumentContext ctx = JsonPath.using(STRICT_PROVIDER_CONFIGURATION).parse(bufferedInputStream);
                    contextHolder.set(ctx);
                }
            }
        });

        return contextHolder.get();
    }

    /**
     * Determines the context by which JsonSmartJsonProvider would treat the value.  {@link java.util.Map} and
     * {@link java.util.List} objects can be rendered as JSON elements, everything else is treated as a scalar.
     *
     * @param obj item to be inspected if it is a scalar or a JSON element
     * @return false, if the object is a supported type; true otherwise
     */
    static boolean isJsonScalar(Object obj) {
        // For the default provider, JsonSmartJsonProvider, a Map or List is able to be handled as a JSON entity
        return !(obj instanceof Map || obj instanceof List);
    }

    static String getResultRepresentation(Object jsonPathResult) {
        if (isJsonScalar(jsonPathResult)) {
            return jsonPathResult.toString();
        }
        return JSON_PROVIDER.toJson(jsonPathResult);
    }

    abstract static class JsonPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            String error = null;
            if (isStale(subject, input)) {
                if (JsonPathExpressionValidator.isValidExpression(input)) {
                    JsonPath compiledJsonPath = JsonPath.compile(input);
                    cacheComputedValue(subject, input, compiledJsonPath);
                } else {
                    error = "specified expression was not valid: " + input;
                }
            }
            return new ValidationResult.Builder().subject(subject).valid(error == null).explanation(error).build();
        }

        /**
         * An optional hook to act on the compute value
         */

        abstract void cacheComputedValue(String subject, String input, JsonPath computedJsonPath);

        /**
         * A hook for implementing classes to determine if a cached value is stale for a compiled JsonPath represented
         * by either a validation
         */
        abstract boolean isStale(String subject, String input);
    }
}
