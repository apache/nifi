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
package org.apache.nifi.processors.yandex;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.yandex.model.Translation;
import org.apache.nifi.processors.yandex.util.Languages;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.web.util.WebUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"yandex", "translate", "translation", "language"})
@CapabilityDescription("Translates content and attributes from one language to another")
@WritesAttributes({
        @WritesAttribute(attribute = "yandex.translate.failure.reason", description = "If the text cannot be translated, this attribute will be set indicating the reason for the failure"),
        @WritesAttribute(attribute = "language", description = "When the translation succeeds, if the content was translated, this attribute will be set indicating the new language of the content")
})
@DynamicProperty(name = "The name of an attribute to set that will contain the translated text of the value",
        value = "The value to translate",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "User-defined properties are used to translate arbitrary text based on attributes.")
public class YandexTranslate extends AbstractProcessor {

    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("Yandex API Key")
            .description("The API Key that is registered with Yandex")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor SOURCE_LANGUAGE = new PropertyDescriptor.Builder()
            .name("Input Language")
            .description("The language of incoming data. If no language is set, Yandex will attempt to detect the incoming language automatically.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new LanguageNameValidator())
            .build();
    public static final PropertyDescriptor TARGET_LANGUAGE = new PropertyDescriptor.Builder()
            .name("Target Language")
            .description("The language to translate the text into")
            .required(true)
            .defaultValue("en")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new LanguageNameValidator())
            .build();
    public static final PropertyDescriptor TRANSLATE_CONTENT = new PropertyDescriptor.Builder()
            .name("Translate Content")
            .description("Specifies whether or not the content should be translated. If false, only the text specified by user-defined properties will be translated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the data to be translated")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("This relationship is used when the translation is successful")
            .build();
    public static final Relationship REL_COMMS_FAILURE = new Relationship.Builder()
            .name("comms.failure")
            .description("This relationship is used when the translation fails due to a problem such as a network failure, and for which the translation should be attempted again")
            .build();
    public static final Relationship REL_TRANSLATION_FAILED = new Relationship.Builder()
            .name("translation.failure")
            .description("This relationship is used if the translation cannot be performed for some reason other than communications failure")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private volatile Client client;

    private static final String URL = "https://translate.yandex.net/api/v1.5/tr.json/translate";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(KEY);
        descriptors.add(SOURCE_LANGUAGE);
        descriptors.add(TARGET_LANGUAGE);
        descriptors.add(TRANSLATE_CONTENT);
        descriptors.add(CHARACTER_SET);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_COMMS_FAILURE);
        relationships.add(REL_TRANSLATION_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        if (validationContext.getProperty(TRANSLATE_CONTENT).asBoolean().equals(Boolean.FALSE)) {
            boolean foundDynamic = false;
            for (final PropertyDescriptor descriptor : validationContext.getProperties().keySet()) {
                if (descriptor.isDynamic()) {
                    foundDynamic = true;
                    break;
                }
            }

            if (!foundDynamic) {
                results.add(new ValidationResult.Builder().subject("Text to translate").input("<none>").valid(false)
                        .explanation("Must either set 'Translate Content' to true or add at least one user-defined property").build());
            }
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        client = WebUtils.createClient(null);
    }

    @OnStopped
    public void destroyClient() {
        if (client != null) {
            client.close();
        }
    }

    protected Invocation prepareResource(final String key, final List<String> text, final String sourceLanguage, final String destLanguage) {
        Invocation.Builder builder = client.target(URL).request(MediaType.APPLICATION_JSON);

        final MultivaluedHashMap entity = new MultivaluedHashMap();
        entity.put("text", text);
        entity.add("key", key);
        if ((StringUtils.isBlank(sourceLanguage))) {
            entity.add("lang", destLanguage);
        } else {
            entity.add("lang", sourceLanguage + "-" + destLanguage);
        }

        return builder.buildPost(Entity.form(entity));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final String key = context.getProperty(KEY).getValue();
        final String sourceLanguage = context.getProperty(SOURCE_LANGUAGE).evaluateAttributeExpressions(flowFile).getValue();
        final String targetLanguage = context.getProperty(TARGET_LANGUAGE).evaluateAttributeExpressions(flowFile).getValue();
        final String encoding = context.getProperty(CHARACTER_SET).evaluateAttributeExpressions(flowFile).getValue();

        final List<String> attributeNames = new ArrayList<>();
        final List<String> textValues = new ArrayList<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                attributeNames.add(descriptor.getName()); // add to list so that we know the order when the translations come back.
                textValues.add(context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue());
            }
        }

        if (context.getProperty(TRANSLATE_CONTENT).asBoolean()) {
            final byte[] buff = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, buff);
                }
            });
            final String content = new String(buff, Charset.forName(encoding));
            textValues.add(content);
        }

        final Invocation invocation = prepareResource(key, textValues, sourceLanguage, targetLanguage);

        final Response response;
        try {
            response = invocation.invoke();
        } catch (final Exception e) {
            getLogger().error("Failed to make request to Yandex to transate text for {} due to {}; routing to comms.failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_COMMS_FAILURE);
            return;
        }

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            getLogger().error("Failed to translate text using Yandex for {}; response was {}: {}; routing to {}", new Object[]{
                    flowFile, response.getStatus(), response.getStatusInfo().getReasonPhrase(), REL_TRANSLATION_FAILED.getName()});
            flowFile = session.putAttribute(flowFile, "yandex.translate.failure.reason", response.getStatusInfo().getReasonPhrase());
            session.transfer(flowFile, REL_TRANSLATION_FAILED);
            return;
        }

        final Map<String, String> newAttributes = new HashMap<>();
        final Translation translation = response.readEntity(Translation.class);
        final List<String> texts = translation.getText();
        for (int i = 0; i < texts.size(); i++) {
            final String text = texts.get(i);
            if (i < attributeNames.size()) {
                final String attributeName = attributeNames.get(i);
                newAttributes.put(attributeName, text);
            } else {
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        out.write(text.getBytes(encoding));
                    }
                });

                newAttributes.put("language", targetLanguage);
            }
        }

        if (!newAttributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, newAttributes);
        }

        stopWatch.stop();
        session.transfer(flowFile, REL_SUCCESS);
        getLogger().info("Successfully translated {} items for {} from {} to {} in {}; routing to success",
                new Object[]{texts.size(), flowFile, sourceLanguage, targetLanguage, stopWatch.getDuration()});
    }

    private static class LanguageNameValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if ((StringUtils.isBlank(input))) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).explanation("No Language Input Present").build();
            }

            if (context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).explanation("Expression Language Present").build();
            }

            if (Languages.getLanguageMap().keySet().contains(input.toLowerCase())) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

            return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(input + " is not a language that is supported by Yandex").build();
        }

    }
}