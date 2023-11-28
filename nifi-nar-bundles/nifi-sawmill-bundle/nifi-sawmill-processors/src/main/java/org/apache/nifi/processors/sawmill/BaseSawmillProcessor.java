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
package org.apache.nifi.processors.sawmill;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.logz.sawmill.GeoIpConfiguration;
import io.logz.sawmill.Pipeline;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public abstract class BaseSawmillProcessor extends AbstractProcessor {

    public static final PropertyDescriptor SAWMILL_TRANSFORM = new PropertyDescriptor.Builder()
            .name("Sawmill Transformation")
            .description("Sawmill Transformation for transform of JSON data. Any NiFi Expression Language present will be evaluated first to get the final transform to be applied. " +
                    "The Sawmill Tutorial provides an overview of supported expressions: https://github.com/logzio/sawmill/wiki")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT, ResourceType.FILE)
            .required(true)
            .build();

    public static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Transform Cache Size")
            .description("Compiling a Sawmill Transform can be fairly expensive. Ideally, this will be done only once. However, if Expression Language is used in the transform, we may need "
                    + "a new Transform for each FlowFile. This value controls how many of those Transforms we cache in memory in order to avoid having to compile the Transform each time.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .required(true)
            .build();

    public static final PropertyDescriptor GEO_DATABASE_FILE = new PropertyDescriptor.Builder()
            .name("MaxMind Database File")
            .description("Path to Maxmind IP Enrichment Database File")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamicallyModifiesClasspath(true)
            .build();

    protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();
    protected volatile Cache<String, Pipeline> transformCache;

    protected volatile AtomicReference<Pipeline.Factory> pipelineFactory = new AtomicReference<>();

    protected void onScheduled(final ProcessContext context) {
        if (context.getProperty(GEO_DATABASE_FILE).isSet()) {
            final String geoIpDbPath = context.getProperty(GEO_DATABASE_FILE).asResource().asFile().getName();
            GeoIpConfiguration geoIpConfiguration = new GeoIpConfiguration(geoIpDbPath);

            try {
                Pipeline.Factory createdFactory = new Pipeline.Factory(geoIpConfiguration);
                pipelineFactory.compareAndSet(null, createdFactory);
            } catch (Throwable t) {
                getLogger().warn("Error finding or reading GeoIP database was specified. GeoIP-related pipeline steps will fail");
                pipelineFactory.compareAndSet(null, new Pipeline.Factory());
            }
        } else {
            getLogger().info("No GeoIP database was specified. GeoIP-related pipeline steps will fail");
            pipelineFactory.compareAndSet(null, new Pipeline.Factory());
        }

        int maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();
        transformCache = Caffeine.newBuilder()
                .maximumSize(maxTransformsToCache)
                .build();
        // Precompile the transform if it hasn't been done already (and if there is no PipelineDefinition Language present)
        final PropertyValue transformProperty = context.getProperty(SAWMILL_TRANSFORM);
        if (!transformProperty.isExpressionLanguagePresent()) {
            try {
                final String transform = readTransform(transformProperty);
                transformCache.put(transform, getSawmillPipelineDefinition(transform));
            } catch (final IOException | RuntimeException e) {
                throw new ProcessException("Sawmill Transform compilation failed", e);
            }
        }
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (descriptor.equals(GEO_DATABASE_FILE)) {
            pipelineFactory.set(null);
        }
    }

    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final String transform = validationContext.getProperty(SAWMILL_TRANSFORM).getValue();

        try {
            final boolean elPresent = validationContext.isExpressionLanguagePresent(transform);

            if (elPresent) {
                final String invalidExpressionMsg = validationContext.newExpressionLanguageCompiler().validateExpression(transform, true);
                if (invalidExpressionMsg != null) {
                    results.add(new ValidationResult.Builder().valid(false)
                            .subject(SAWMILL_TRANSFORM.getDisplayName())
                            .explanation("Invalid Expression Language: " + invalidExpressionMsg)
                            .build());
                }
            } else {
                // For validation we want to be able to ensure the spec is syntactically correct and not try to resolve variables since they may not exist yet
                try {
                    final Pipeline.Factory testFactory;
                    if (validationContext.getProperty(GEO_DATABASE_FILE).isSet()) {
                        final String geoIpDbPath = validationContext.getProperty(GEO_DATABASE_FILE).asResource().asFile().getName();
                        GeoIpConfiguration geoIpConfiguration = new GeoIpConfiguration(geoIpDbPath);
                        // Try to instantiate a factory with a GeoIP database location
                        testFactory = new Pipeline.Factory(geoIpConfiguration);
                    } else {
                        // Try to instantiate a factory
                        testFactory = new Pipeline.Factory();
                    }
                    final String content = readTransform(validationContext.getProperty(SAWMILL_TRANSFORM));
                    testFactory.create(content);
                } catch (Throwable t) {
                    results.add(new ValidationResult.Builder().valid(false)
                            .subject(SAWMILL_TRANSFORM.getDisplayName())
                            .explanation(t.getCause() != null ? t.getCause().getMessage() :  t.getMessage())
                            .build());
                }
            }
        } catch (final Exception e) {
            getLogger().info("Processor is not valid - ", e);
            String message = "Specification not valid for the selected transformation.";
            results.add(new ValidationResult.Builder().valid(false)
                    .explanation(message)
                    .build());
        }
        return results;
    }

    protected Pipeline getSawmillPipelineDefinition(String transform) {
        return pipelineFactory.get().create(transform);
    }

    protected String readTransform(final PropertyValue propertyValue, final FlowFile flowFile) throws IOException {
        final String transform;

        if (propertyValue.isExpressionLanguagePresent()) {
            transform = propertyValue.evaluateAttributeExpressions(flowFile).getValue();
        } else {
            transform = readTransform(propertyValue);
        }

        return transform;
    }

    protected String readTransform(final PropertyValue propertyValue) throws IOException {
        final ResourceReference resourceReference = propertyValue.asResource();
        if (resourceReference == null) {
            return propertyValue.getValue();
        }
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(resourceReference.read()))) {
            return reader.lines().collect(Collectors.joining());
        }
    }
}