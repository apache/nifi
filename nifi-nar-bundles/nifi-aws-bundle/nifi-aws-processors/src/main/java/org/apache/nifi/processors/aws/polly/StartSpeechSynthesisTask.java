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

package org.apache.nifi.processors.aws.polly;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.polly.AmazonPollyAsync;
import com.amazonaws.services.polly.AmazonPollyAsyncClient;
import com.amazonaws.services.polly.AmazonPollyAsyncClientBuilder;
import com.amazonaws.services.polly.model.EngineNotSupportedException;
import com.amazonaws.services.polly.model.InvalidS3BucketException;
import com.amazonaws.services.polly.model.InvalidS3KeyException;
import com.amazonaws.services.polly.model.InvalidSampleRateException;
import com.amazonaws.services.polly.model.InvalidSnsTopicArnException;
import com.amazonaws.services.polly.model.InvalidSsmlException;
import com.amazonaws.services.polly.model.LanguageNotSupportedException;
import com.amazonaws.services.polly.model.LexiconNotFoundException;
import com.amazonaws.services.polly.model.MarksNotSupportedForFormatException;
import com.amazonaws.services.polly.model.SsmlMarksNotSupportedForTextTypeException;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.TextLengthExceededException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@CapabilityDescription("Sends the contents of an incoming FlowFile to Amazon Polly service in order to synthesize speech from the FlowFile's textual content. This processor does not wait for the " +
    "results but rather starts the task. Once the task has started, the outgoing FlowFile will have attributes added pointing to the Amazon Polly Task. Those results can then be fetched using the " +
    "FetchSpeechSynthesisResults. See this Processor's Additional Details for more information.")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.polly.task.id", description = "The ID of the Amazon Polly task that was created. This can be used by FetchSpeechSynthesisResults to get the results of this " +
        "task."),
    @WritesAttribute(attribute = "aws.polly.task.creation.time", description =  "The timestamp (in milliseconds since epoch) that the Amazon Polly task was created.")
})
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"aws", "cloud", "text", "polly", "ml", "ai", "machine learning", "artificial intelligence", "speech", "text-to-speech", "unstructured"})
@SeeAlso({FetchSpeechSynthesisResults.class})
public class StartSpeechSynthesisTask extends AbstractAWSCredentialsProviderProcessor<AmazonPollyAsyncClient> {
    private static final String FAILURE_REASON_ATTRIBUTE = "failure.reason";

    private static final AllowableValue TEXT_TYPE_SSML = new AllowableValue("ssml", "Speech Synthesis Markup Language (SSML)", "Input is in SSML format");
    private static final AllowableValue TEXT_TYPE_PLAIN_TEXT = new AllowableValue("text", "Plain Text", "Input is in plain text format");
    private static final AllowableValue TEXT_TYPE_USE_ATTRIBUTE = new AllowableValue("attribute", "Use 'mime.type' Attribute", "The mime.type attribute will be used to determine whether the input " +
        "is in SSML format or plain text. If the attribute is not present or one of the known MIME types for SSML, then plain text will be used.");
    private static final Set<String> SSML_MIME_TYPES = new HashSet<>(Arrays.asList("application/voicexml+xml", "application/ssml+xml", "application/srgs",
        "application/srgs+xml", "application/ccxml+xml", "application/pls+xml"));

    private static final String VOICE_ID_ATTRIBUTE = "aws.voice.id";
    private static final String USE_VOICE_ID_ATTRIBUTE = "Use '" + VOICE_ID_ATTRIBUTE + "' Attribute";

    static final PropertyDescriptor TEXT_TYPE = new PropertyDescriptor.Builder()
        .name("Text Type")
        .displayName("Text Type")
        .description("The format of the FlowFile content")
        .required(true)
        .allowableValues(TEXT_TYPE_SSML, TEXT_TYPE_PLAIN_TEXT, TEXT_TYPE_USE_ATTRIBUTE)
        .defaultValue(TEXT_TYPE_USE_ATTRIBUTE.getValue())
        .build();
    static final PropertyDescriptor VOICE_ID = new PropertyDescriptor.Builder()
        .name("Voice ID")
        .displayName("Voice ID")
        .description("The identifier for which of the voices should be used to synthesize speech")
        .required(true)
        .allowableValues("Aditi", "Amy", "Astrid", "Bianca", "Brian", "Camila", "Carla", "Carmen", "Celine", "Chantal", "Conchita", "Cristiano",
            "Dora", "Emma", "Enrique", "Ewa", "Filiz", "Gabrielle", "Geraint", "Giorgio", "Gwyneth", "Hans", "Ines", "Ivy",
            "Jacek", "Jan", "Joanna", "Joey", "Justin", "Karl", "Kendra", "Kevin", "Kimberly", "Lea", "Liv", "Lotte", "Lucia", "Lupe",
            "Mads", "Maja", "Marlene", "Mathieu", "Matthew", "Maxim", "Mia", "Miguel", "Mizuki", "Naja", "Nicole", "Olivia",
            "Penelope", "Raveena", "Ricardo", "Ruben", "Russell", "Salli", "Seoyeon", "Takumi", "Tatyana", "Vicki", "Vitoria",
            "Zeina", "Zhiyu", "Aria", "Ayanda", USE_VOICE_ID_ATTRIBUTE)
        .defaultValue("Amy")
        .build();
    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .displayName("Character Set")
        .description("The character set of the FlowFile content")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("UTF-8")
        .build();
    static final PropertyDescriptor ENGINE = new PropertyDescriptor.Builder()
        .name("Engine")
        .displayName("Engine")
        .description("Which Engine to use for performing the speech synthesis")
        .required(true)
        .allowableValues("standard", "neural")
        .defaultValue("standard")
        .build();
    static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor.Builder()
        .name("Output Format")
        .displayName("Output Format")
        .description("Specifies the format that Amazon Polly should generate")
        .required(true)
        .allowableValues("json", "mp3", "ogg_vorbis", "pcm")
        .defaultValue("mp3")
        .build();
    static final PropertyDescriptor OUTPUT_S3_BUCKET_NAME = new PropertyDescriptor.Builder()
        .name("Output S3 Bucket Name")
        .displayName("Output S3 Bucket Name")
        .description("The name of the S3 bucket that Polly should write the results to")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor OUTPUT_S3_KEY_PREFIX = new PropertyDescriptor.Builder()
        .name("Output S3 Key Prefix")
        .displayName("Output S3 Key Prefix")
        .description("The prefix that Polly should use for naming the S3 Object that it writes the results to")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor SAMPLE_RATE = new PropertyDescriptor.Builder()
        .name("Sample Rate")
        .displayName("Sample Rate")
        .description("The Sample Rate that should be used for synthesizing speech")
        .required(true)
        .allowableValues("8000", "16000", "22050", "24000")
        .defaultValue("22050")
        .build();
    static final PropertyDescriptor SNS_TOPIC_ARN = new PropertyDescriptor.Builder()
        .name("SNS Topic ARN")
        .displayName("SNS Topic ARN")
        .description("If specified, Polly will place a notification on the SNS topic whose ARN is specified when the speech synthesis is complete.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor SPEECH_MARK_TYPES = new PropertyDescriptor.Builder()
        .name("Speech Mark Types")
        .displayName("Speech Mark Types")
        .description("A comma-separted list of Speech Mark Types that should be returned for the input text. Valid values are 'sentence', 'ssml', 'viseme', and 'word'")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .dependsOn(OUTPUT_FORMAT, "json")
        .build();
    static final PropertyDescriptor LANGUAGE_CODE = new PropertyDescriptor.Builder()
        .name("Language Code")
        .displayName("Language Code")
        .description("The Language Code to use for speech synthesis if the selected voice is bilingual")
        .required(false)
        .allowableValues("arb", "cmn-CN", "cy-GB", "da-DK", "de-DE", "en-AU", "en-GB", "en-GB-WLS", "en-IN", "en-US", "es-ES", "es-MX", "es-US",
            "fr-CA", "fr-FR", "is-IS", "it-IT", "ja-JP", "hi-IN", "ko-KR", "nb-NO", "nl-NL", "pl-PL", "pt-BR", "pt-PT", "ro-RO", "ru-RU", "sv-SE",
            "tr-TR", "en-NZ", "en-ZA")
        .build();
    static final PropertyDescriptor LEXICON_NAMES = new PropertyDescriptor.Builder()
        .name("Lexicon Names")
        .displayName("Lexicon Names")
        .description("A comma-separated list of Lexicon Names that should be used for speech synthesis")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        TEXT_TYPE,
        VOICE_ID,
        CHARACTER_SET,
        ENGINE,
        OUTPUT_FORMAT,
        OUTPUT_S3_BUCKET_NAME,
        OUTPUT_S3_KEY_PREFIX,
        SAMPLE_RATE,
        SNS_TOPIC_ARN,
        SPEECH_MARK_TYPES,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        REGION,
        LANGUAGE_CODE,
        LEXICON_NAMES,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        PROXY_HOST,
        PROXY_HOST_PORT,
        PROXY_USERNAME,
        PROXY_PASSWORD
    ));

    // Relationships
    static final Relationship REL_INVALID_INPUT = new Relationship.Builder()
        .name("invalid input")
        .description("Amazon Polly indicated that the input was not valid for some reason - for instance, if the document cannot be found in S3.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("This relationship is used for any authentication or authorization failure or if any other unexpected failure is encountered.")
        .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS,
        REL_INVALID_INPUT,
        REL_FAILURE)));


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected AmazonPollyAsyncClient createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        final String region = context.getProperty(REGION).getValue();
        final AmazonPollyAsync client = AmazonPollyAsyncClientBuilder.standard()
            .withClientConfiguration(config)
            .withCredentials(credentialsProvider)
            .withRegion(region)
            .build();

        return (AmazonPollyAsyncClient) client;
    }

    @Override
    protected boolean isInitializeRegionAndEndpoint() {
        return false;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StartSpeechSynthesisTaskRequest request;
        final String characterSet = context.getProperty(CHARACTER_SET).evaluateAttributeExpressions(flowFile).getValue();
        try {
            request = createRequest(context, session, flowFile, characterSet);
        } catch (final IOException e) {
            getLogger().error("Could not read FlowFile content as text using the {} character set. Routing to failure.", characterSet, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final StartSpeechSynthesisTaskResult result;
        final Future<StartSpeechSynthesisTaskResult> resultFuture = client.startSpeechSynthesisTaskAsync(request);
        try {
            try {
                result = resultFuture.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                getLogger().error("Interrupted while waiting for Polly to start speech synthesis job for {}. Will route to failure.", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
                return;
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        } catch (final EngineNotSupportedException | InvalidS3BucketException | InvalidS3KeyException | InvalidSampleRateException | InvalidSnsTopicArnException |
                    InvalidSsmlException | LanguageNotSupportedException | LexiconNotFoundException | MarksNotSupportedForFormatException |
                    SsmlMarksNotSupportedForTextTypeException | TextLengthExceededException e) {

            getLogger().error("Failed to start job for {}. Routing to {}", flowFile, REL_INVALID_INPUT.getName(), e);
            flowFile = session.penalize(flowFile);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_INVALID_INPUT);
            return;
        } catch (final Throwable t) {
            getLogger().error("Failed to start job for {}. Routing to {}", flowFile, REL_FAILURE.getName(), t);
            flowFile = session.penalize(flowFile);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, t.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("aws.polly.task.creation.time", String.valueOf(result.getSynthesisTask().getCreationTime().getTime()));
        attributes.put("aws.polly.task.id", result.getSynthesisTask().getTaskId());

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);

        session.getProvenanceReporter().invokeRemoteProcess(flowFile, "https://polly.amazonaws.com/tasks/" + result.getSynthesisTask().getTaskId());
    }

    private StartSpeechSynthesisTaskRequest createRequest(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final String characterSet) throws IOException {
        final String text;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) flowFile.getSize())) {
            session.exportTo(flowFile, baos);
            final byte[] flowFileContents = baos.toByteArray();
            text = new String(flowFileContents, characterSet);
        }

        final String rawTextType = context.getProperty(TEXT_TYPE).getValue();
        String textType = rawTextType;
        if (TEXT_TYPE_USE_ATTRIBUTE.getValue().equalsIgnoreCase(rawTextType)) {
            final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
            if (SSML_MIME_TYPES.contains(mimeType)) {
                textType = "ssml";
            } else {
                textType = "text";
            }
        }

        final String rawVoiceId = context.getProperty(VOICE_ID).getValue();
        final String voiceId;
        if (USE_VOICE_ID_ATTRIBUTE.equalsIgnoreCase(rawVoiceId)) {
            voiceId = flowFile.getAttribute(VOICE_ID_ATTRIBUTE);
        } else {
            voiceId = rawVoiceId;
        }

        final StartSpeechSynthesisTaskRequest request = new StartSpeechSynthesisTaskRequest();
        request.setEngine(context.getProperty(ENGINE).getValue());
        request.setLanguageCode(context.getProperty(LANGUAGE_CODE).getValue());
        request.setLexiconNames(toCollection(context.getProperty(LEXICON_NAMES).getValue()));
        request.setOutputFormat(context.getProperty(OUTPUT_FORMAT).getValue());
        request.setOutputS3BucketName(context.getProperty(OUTPUT_S3_BUCKET_NAME).evaluateAttributeExpressions(flowFile).getValue());
        request.setOutputS3KeyPrefix(context.getProperty(OUTPUT_S3_KEY_PREFIX).evaluateAttributeExpressions(flowFile).getValue());
        request.setSampleRate(context.getProperty(SAMPLE_RATE).getValue());
        request.setSnsTopicArn(context.getProperty(SNS_TOPIC_ARN).evaluateAttributeExpressions(flowFile).getValue());
        request.setSpeechMarkTypes("json".equals(context.getProperty(OUTPUT_FORMAT).getValue()) ? toCollection(context.getProperty(SPEECH_MARK_TYPES).getValue()) : null);
        request.setText(text);
        request.setTextType(textType);
        request.setVoiceId(voiceId);

        return request;
    }

    private Collection<String> toCollection(final String value) {
        if (value == null || value.trim().equals("")) {
            return null;
        }

        final String[] splits = value.split(",");
        final List<String> collection = new ArrayList<>();
        for (final String split : splits) {
            final String trimmed = split.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            collection.add(trimmed);
        }

        return collection;
    }

    @Override
    protected AmazonPollyAsyncClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        return null;
    }
}
