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
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.util.StringUtils;

/**
 * Provides common functionality used for processors interacting and manipulating JSON data via JsonPath.
 *
 * @see <a href="http://json.org">http://json.org</a>
 * @see
 * <a href="https://github.com/jayway/JsonPath">https://github.com/jayway/JsonPath</a>
 */
public abstract class AbstractJsonPathProcessor extends AbstractProcessor {

    private static final Configuration STRICT_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();

    private static final JsonProvider JSON_PROVIDER = STRICT_PROVIDER_CONFIGURATION.jsonProvider();

    static final Map<String, String> NULL_REPRESENTATION_MAP = new HashMap<>();

    static final String EMPTY_STRING_OPTION = "empty string";
    static final String NULL_STRING_OPTION = "the string 'null'";

    static {
        NULL_REPRESENTATION_MAP.put(EMPTY_STRING_OPTION, "");
        NULL_REPRESENTATION_MAP.put(NULL_STRING_OPTION, "null");
    }

    public static final PropertyDescriptor NULL_VALUE_DEFAULT_REPRESENTATION = new PropertyDescriptor.Builder()
            .name("Null Value Representation")
            .description("Indicates the desired representation of JSON Path expressions resulting in a null value.")
            .required(true)
            .allowableValues(NULL_REPRESENTATION_MAP.keySet())
            .defaultValue(EMPTY_STRING_OPTION)
            .build();

    static DocumentContext validateAndEstablishJsonContext(ProcessSession processSession, FlowFile flowFile) {
        // Parse the document once into an associated context to support multiple path evaluations if specified
        final AtomicReference<DocumentContext> contextHolder = new AtomicReference<>(null);
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
     * Determines the context by which JsonSmartJsonProvider would treat the value. {@link java.util.Map} and {@link java.util.List} objects can be rendered as JSON elements, everything else is
     * treated as a scalar.
     *
     * @param obj item to be inspected if it is a scalar or a JSON element
     * @return false, if the object is a supported type; true otherwise
     */
    static boolean isJsonScalar(Object obj) {
        // For the default provider, JsonSmartJsonProvider, a Map or List is able to be handled as a JSON entity
        return !(obj instanceof Map || obj instanceof List);
    }

    static String getResultRepresentation(Object jsonPathResult, String defaultValue) {
        if (isJsonScalar(jsonPathResult)) {
            return Objects.toString(jsonPathResult, defaultValue);
        }
        return JSON_PROVIDER.toJson(jsonPathResult);
    }

    abstract static class JsonPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            String error = null;
            if (isStale(subject, input)) {
                if (!StringUtils.isBlank(input)) {
                    try {
                        JsonPath compiledJsonPath = JsonPath.compile(input);
                        cacheComputedValue(subject, input, compiledJsonPath);
                    } catch (Exception ex) {
                        error = String.format("specified expression was not valid: %s", input);
                    }
                } else {
                    error = "the expression cannot be empty.";
                }
            }
            return new ValidationResult.Builder().subject(subject).valid(error == null).explanation(error).build();
        }

        /**
         * An optional hook to act on the compute value
         */
        abstract void cacheComputedValue(String subject, String input, JsonPath computedJsonPath);

        /**
         * A hook for implementing classes to determine if a cached value is stale for a compiled JsonPath represented by either a validation
         */
        abstract boolean isStale(String subject, String input);
    }
}
