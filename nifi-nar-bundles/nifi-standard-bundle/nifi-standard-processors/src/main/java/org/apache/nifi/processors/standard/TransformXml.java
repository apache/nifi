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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.XMLConstants;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"xml", "xslt", "transform"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Applies the provided XSLT file to the flowfile XML payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the XSL transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship")
@DynamicProperty(name = "An XSLT transform parameter name", value = "An XSLT transform parameter value",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "These XSLT parameters are passed to the transformer")
public class TransformXml extends AbstractProcessor {

    public static final PropertyDescriptor XSLT_FILE_NAME = new PropertyDescriptor.Builder()
            .name("XSLT file name")
            .description("Provides the name (including full path) of the XSLT file to apply to the flowfile XML content."
                    + "One of the 'XSLT file name' and 'XSLT Lookup' properties must be defined.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor XSLT_CONTROLLER = new PropertyDescriptor.Builder()
            .name("xslt-controller")
            .displayName("XSLT Lookup")
            .description("Controller lookup used to store XSLT definitions. One of the 'XSLT file name' and "
                    + "'XSLT Lookup' properties must be defined. WARNING: note that the lookup controller service "
                    + "should not be used to store large XSLT files.")
            .required(false)
            .identifiesControllerService(StringLookupService.class)
            .build();

    public static final PropertyDescriptor XSLT_CONTROLLER_KEY = new PropertyDescriptor.Builder()
            .name("xslt-controller-key")
            .displayName("XSLT Lookup key")
            .description("Key used to retrieve the XSLT definition from the XSLT lookup controller. This property must be "
                    + "set when using the XSLT controller property.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDENT_OUTPUT = new PropertyDescriptor.Builder()
            .name("indent-output")
            .displayName("Indent")
            .description("Whether or not to indent the output.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECURE_PROCESSING = new PropertyDescriptor.Builder()
            .name("secure-processing")
            .displayName("Secure processing")
            .description("Whether or not to mitigate various XML-related attacks like XXE (XML External Entity) attacks.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache size")
            .description("Maximum number of stylesheets to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_LAST_ACCESS = new PropertyDescriptor.Builder()
            .name("cache-ttl-after-last-access")
            .displayName("Cache TTL after last access")
            .description("The cache TTL (time-to-live) or how long to keep stylesheets in the cache after last access.")
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid XML), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private LoadingCache<String, Templates> cache;

    private static AtomicReference<LookupService<String>> lookupService = new AtomicReference<LookupService<String>>(null);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(XSLT_FILE_NAME);
        properties.add(XSLT_CONTROLLER);
        properties.add(XSLT_CONTROLLER_KEY);
        properties.add(INDENT_OUTPUT);
        properties.add(SECURE_PROCESSING);
        properties.add(CACHE_SIZE);
        properties.add(CACHE_TTL_AFTER_LAST_ACCESS);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        PropertyValue filename = validationContext.getProperty(XSLT_FILE_NAME);
        PropertyValue controller = validationContext.getProperty(XSLT_CONTROLLER);
        PropertyValue key = validationContext.getProperty(XSLT_CONTROLLER_KEY);

        if((filename.isSet() && controller.isSet())
                || (!filename.isSet() && !controller.isSet())) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(this.getClass().getSimpleName())
                    .explanation("Exactly one of the \"XSLT file name\" and \"XSLT controller\" properties must be defined.")
                    .build());
        }

        if(controller.isSet() && !key.isSet()) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(XSLT_CONTROLLER_KEY.getDisplayName())
                    .explanation("If using \"XSLT controller\", the XSLT controller key property must be defined.")
                    .build());
        }

        if(controller.isSet()) {
            final LookupService<String> lookupService = validationContext.getProperty(XSLT_CONTROLLER).asControllerService(StringLookupService.class);
            final Set<String> requiredKeys = lookupService.getRequiredKeys();
            if (requiredKeys == null || requiredKeys.size() != 1) {
                results.add(new ValidationResult.Builder()
                        .valid(false)
                        .subject(XSLT_CONTROLLER.getDisplayName())
                        .explanation("This processor requires a key-value lookup service supporting exactly one required key, was: " +
                            (requiredKeys == null ? "null" : String.valueOf(requiredKeys.size())))
                        .build());
            }
        }

        return results;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .required(false)
                .dynamic(true)
                .build();
    }

    private Templates newTemplates(final ProcessContext context, final String path) throws TransformerConfigurationException, LookupFailureException {
        final Boolean secureProcessing = context.getProperty(SECURE_PROCESSING).asBoolean();
        TransformerFactory factory = TransformerFactory.newInstance();
        final boolean isFilename = context.getProperty(XSLT_FILE_NAME).isSet();

        if (secureProcessing) {
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            // don't be overly DTD-unfriendly forcing http://apache.org/xml/features/disallow-doctype-decl
            factory.setFeature("http://saxon.sf.net/feature/parserFeature?uri=http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://saxon.sf.net/feature/parserFeature?uri=http://xml.org/sax/features/external-general-entities", false);
        }

        if(isFilename) {
            return factory.newTemplates(new StreamSource(path));
        } else {
            final String coordinateKey = lookupService.get().getRequiredKeys().iterator().next();
            final Optional<String> attributeValue = lookupService.get().lookup(Collections.singletonMap(coordinateKey, path));
            if (attributeValue.isPresent() && StringUtils.isNotBlank(attributeValue.get())) {
                return factory.newTemplates(new StreamSource(new ByteArrayInputStream(attributeValue.get().getBytes(StandardCharsets.UTF_8))));
            } else {
                throw new TransformerConfigurationException("No XSLT definition is associated to " + path + " in the lookup controller service.");
            }
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        final Integer cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(CACHE_TTL_AFTER_LAST_ACCESS).asTimePeriod(TimeUnit.SECONDS);

        if (cacheSize > 0) {
            CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheTTL > 0) {
                cacheBuilder = cacheBuilder.expireAfterAccess(cacheTTL, TimeUnit.SECONDS);
            }

            cache = cacheBuilder.build(
                    new CacheLoader<String, Templates>() {
                        @Override
                        public Templates load(String path) throws TransformerConfigurationException, LookupFailureException {
                            return newTemplates(context, path);
                        }
                    });
        } else {
            cache = null;
            logger.info("Stylesheet cache disabled because cache size is set to 0");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
        final String path = context.getProperty(XSLT_FILE_NAME).isSet()
                ? context.getProperty(XSLT_FILE_NAME).evaluateAttributeExpressions(original).getValue()
                        : context.getProperty(XSLT_CONTROLLER_KEY).evaluateAttributeExpressions(original).getValue();
        final Boolean indentOutput = context.getProperty(INDENT_OUTPUT).asBoolean();
        lookupService.set(context.getProperty(XSLT_CONTROLLER).asControllerService(LookupService.class));

        try {
            FlowFile transformed = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream out) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        final Templates templates;
                        if (cache != null) {
                            templates = cache.get(path);
                        } else {
                            templates = newTemplates(context, path);
                        }

                        final Transformer transformer = templates.newTransformer();
                        transformer.setOutputProperty(OutputKeys.INDENT, (indentOutput ? "yes" : "no"));

                        // pass all dynamic properties to the transformer
                        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                            if (entry.getKey().isDynamic()) {
                                String value = context.newPropertyValue(entry.getValue()).evaluateAttributeExpressions(original).getValue();
                                transformer.setParameter(entry.getKey().getName(), value);
                            }
                        }

                        // use a StreamSource with Saxon
                        StreamSource source = new StreamSource(in);
                        StreamResult result = new StreamResult(out);
                        transformer.transform(source, result);
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }
                }
            });
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.info("Transformed {}", new Object[]{original});
        } catch (ProcessException e) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        }
    }

    @SuppressWarnings("unused")
    private static final class XsltValidator implements Validator {

        private volatile Tuple<String, ValidationResult> cachedResult;

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            final Tuple<String, ValidationResult> lastResult = this.cachedResult;
            if (lastResult != null && lastResult.getKey().equals(input)) {
                return lastResult.getValue();
            } else {
                String error = null;
                final File stylesheet = new File(input);
                final TransformerFactory tFactory = new net.sf.saxon.TransformerFactoryImpl();
                final StreamSource styleSource = new StreamSource(stylesheet);

                try {
                    tFactory.newTransformer(styleSource);
                } catch (final Exception e) {
                    error = e.toString();
                }

                this.cachedResult = new Tuple<>(input, new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(error == null)
                        .explanation(error)
                        .build());
                return this.cachedResult.getValue();
            }
        }
    }

}