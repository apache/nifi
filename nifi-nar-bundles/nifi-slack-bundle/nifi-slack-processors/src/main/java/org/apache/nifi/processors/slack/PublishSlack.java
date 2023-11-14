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
package org.apache.nifi.processors.slack;

import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.SlackFilesUploadV2Exception;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.conversations.ConversationsListRequest;
import com.slack.api.methods.request.files.FilesUploadV2Request;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.conversations.ConversationsListResponse;
import com.slack.api.methods.response.files.FilesUploadV2Response;
import com.slack.api.model.Conversation;
import com.slack.api.model.File;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.slack.consume.ConsumeSlackUtil;
import org.apache.nifi.processors.slack.publish.ContentMessageStrategy;
import org.apache.nifi.processors.slack.publish.MessageOnlyStrategy;
import org.apache.nifi.processors.slack.publish.PublishConfig;
import org.apache.nifi.processors.slack.publish.PublishSlackClient;
import org.apache.nifi.processors.slack.publish.PublishStrategy;
import org.apache.nifi.processors.slack.publish.UploadAttachmentStrategy;
import org.apache.nifi.processors.slack.publish.UploadLinkStrategy;
import org.apache.nifi.processors.slack.publish.UploadOnlyStrategy;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.nifi.util.StringUtils.isEmpty;


@Tags({"slack", "publish", "notify", "upload", "message"})
@CapabilityDescription("Uploads the FlowFile content (e.g. an image) as a file, and/or Sends a message on Slack. The FlowFile content can be used as the message text or attached to the message.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "<Arbitrary name>", value = "JSON snippet specifying a Slack message \"attachment\"",
        description = "The property value will be converted to JSON and will be added to the array of attachments in the JSON payload being sent to Slack." +
                " The property name will not be used by the processor.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttribute(attribute="slack.file.url", description = "The Slack URL of the uploaded file. It will be added if 'Upload FlowFile' has been set to 'Yes'.")
public class PublishSlack extends AbstractProcessor {

    private static final String UPLOAD_ONLY = "upload-only";
    private static final String MESSAGE_ONLY = "message-only";
    private static final String CONTENT_MESSAGE = "content-message";
    private static final String UPLOAD_ATTACH = "upload-attach";
    private static final String UPLOAD_LINK = "upload-link";

    public static final PropertyDescriptor BOT_TOKEN = new PropertyDescriptor.Builder()
            .name("Bot Token")
            .description("The Bot Token that is registered to your Slack application")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    public static final AllowableValue STRATEGY_UPLOAD_ONLY = new AllowableValue(
            UPLOAD_ONLY,
            "Upload File Content Only",
            "Only upload the FlowFile content - it will be a visible Slack message if 'Upload Channel' is specified."
    );

    public static final AllowableValue STRATEGY_MESSAGE_ONLY = new AllowableValue(
            MESSAGE_ONLY,
            "Post Message Only",
            "Post a simple Slack message with optional 'Text' and/or dynamic Properties as attachments."
    );

    public static final AllowableValue STRATEGY_CONTENT_MESSAGE = new AllowableValue(
            CONTENT_MESSAGE,
            "Post File Content as Message Text",
            "Post the FlowFile content as a Slack message with optional dynamic Properties as attachments."
    );

    public static final AllowableValue STRATEGY_UPLOAD_ATTACH = new AllowableValue(
            UPLOAD_ATTACH,
            "Upload File Content as Attachment to Message",
            "Upload the FlowFile content and add a link to the uploaded file as an attachment to the posted Slack message. The message will include optional dynamic Properties as attachments."
    );

    public static final AllowableValue STRATEGY_UPLOAD_LINK = new AllowableValue(
            UPLOAD_LINK,
            "Upload File Content as Embedded Link",
            "Upload the FlowFile content and append a link to the uploaded file in the posted Slack message. The message will include optional dynamic Properties as attachments."
    );

    public static final PropertyDescriptor PUBLISH_STRATEGY = new PropertyDescriptor.Builder()
            .name("publish-strategy")
            .displayName("Publish Strategy")
            .description("Determines how the FlowFile is sent to Slack and how it appears.")
            .allowableValues(STRATEGY_UPLOAD_ONLY, STRATEGY_MESSAGE_ONLY, STRATEGY_CONTENT_MESSAGE, STRATEGY_UPLOAD_ATTACH, STRATEGY_UPLOAD_LINK)
            .required(true)
            .defaultValue(UPLOAD_ATTACH)
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor.Builder()
            .name("channel")
            .displayName("Channel")
            .description("Slack channel, private group, or IM channel to post the message to.")
            .dependsOn(PUBLISH_STRATEGY, MESSAGE_ONLY, CONTENT_MESSAGE, UPLOAD_ATTACH, UPLOAD_LINK)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor UPLOAD_CHANNEL = new PropertyDescriptor.Builder()
            .name("upload-channel")
            .displayName("Upload Channel")
            .description("Slack channel, private group, or IM channel to upload the file to.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor THREAD_TS = new PropertyDescriptor.Builder()
            .name("Thread Timestamp")
            .description("The timestamp of the parent message, also known as a thread_ts, or Thread Timestamp. If not specified, the message will be send to the channel " +
                    "as an independent message. If this value is populated, the message will be sent as a reply to the message whose timestamp is equal to the given value.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEXT = new PropertyDescriptor.Builder()
            .name("text")
            .displayName("Text")
            .description("Text of the Slack message to send. Only required if no attachment has been specified and 'Upload File' has been set to 'No'.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor INITIAL_COMMENT = new PropertyDescriptor.Builder()
            .name("upload-text")
            .displayName("Upload Text")
            .description("Text of the Slack message to include with a public upload. Only used if 'Publish Mode' has been set to 'Upload Only' and 'Upload Channel' is specified.")
            .dependsOn(PUBLISH_STRATEGY, UPLOAD_ONLY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILE_TITLE = new PropertyDescriptor.Builder()
            .name("file-title")
            .displayName("File Title")
            .description("Title of the file displayed in the Slack message." +
                    " The property value will only be used if 'Upload FlowFile' has been set to 'Yes'.")
            .dependsOn(PUBLISH_STRATEGY, UPLOAD_ONLY, UPLOAD_ATTACH, UPLOAD_LINK)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("file-name")
            .displayName("File Name")
            .description("Name of the file to be uploaded." +
                    " The property value will only be used if 'Upload FlowFile' has been set to 'Yes'." +
                    " If the property evaluated to null or empty string, then the file name will be set to 'file' in the Slack message.")
            .dependsOn(PUBLISH_STRATEGY, UPLOAD_ONLY, UPLOAD_ATTACH, UPLOAD_LINK)
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ICON_URL = new PropertyDescriptor
            .Builder()
            .name("icon-url")
            .displayName("Icon URL")
            .description("Icon URL to be used for the message")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ICON_EMOJI = new PropertyDescriptor
            .Builder()
            .name("icon-emoji")
            .displayName("Icon Emoji")
            .description("Icon Emoji to be used for the message. Must begin and end with a colon, e.g. :ghost:")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(new EmojiValidator())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to Slack")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to Slack")
            .build();

    public static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE);

    public static final List<PropertyDescriptor> properties = List.of(
            BOT_TOKEN,
            CHANNEL,
            UPLOAD_CHANNEL,
            THREAD_TS,
            PUBLISH_STRATEGY,
            TEXT,
            INITIAL_COMMENT,
            FILE_TITLE,
            FILE_NAME,
            ICON_URL,
            ICON_EMOJI);

    private final SortedSet<PropertyDescriptor> attachmentProperties = Collections.synchronizedSortedSet(new TreeSet<>());


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Slack Attachment JSON snippet that will be added to the message. The property value will be used unless 'Publish Mode' has been set to 'Upload Only'" +
                        " and no 'Upload Channel' is provided. If the property evaluated to null or empty string, or contains invalid JSON, then it will not be added to the Slack message.")
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> validationResults = new ArrayList<>();

        PublishStrategy strategy;
        try {
            strategy = strategyFor(validationContext.getProperty(PUBLISH_STRATEGY).getValue());
        } catch (RuntimeException e) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(PUBLISH_STRATEGY.getDisplayName())
                    .valid(false)
                    .explanation("unrecognized 'Publish Strategy' - must be one of the defined strategies.")
                    .build());
            return validationResults;
        }
        boolean channelSpecified = validationContext.getProperty(CHANNEL).isSet();
        boolean textSpecified = validationContext.getProperty(TEXT).isSet();
        boolean attachmentSpecified = validationContext.getProperties().keySet()
                .stream()
                .anyMatch(PropertyDescriptor::isDynamic);
        boolean uploadFileYes = strategy.willUploadFile();
        boolean flowFileAsContentYes = strategy.willSendContent() && !strategy.willUploadFile();

        if (!channelSpecified && strategy.willPostMessage()) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(CHANNEL.getDisplayName())
                    .valid(false)
                    .explanation("it is required unless 'Publish Strategy' is set to 'Upload File Content Only'.")
                    .build());
        }
        if (!textSpecified && !attachmentSpecified && !uploadFileYes && !flowFileAsContentYes) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(TEXT.getDisplayName())
                    .valid(false)
                    .explanation("it is required if no attachment has been specified, and 'Publish Strategy' has been set to 'Post Message Only'.")
                    .build());
        }

        return validationResults;
    }

    protected PublishSlackClient initializeClient(final MethodsClient client) {
        return new PublishSlack.DelegatingSlackClient(client);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        attachmentProperties.clear();
        attachmentProperties.addAll(
                context.getProperties().keySet().stream().filter(PropertyDescriptor::isDynamic).toList());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String botToken = context.getProperty(BOT_TOKEN).getValue();
        final AppConfig appConfig = AppConfig.builder().singleTeamBotToken(botToken).build();

        // The Bolt App's MethodsClient uses a thread pool...perhaps we should use the SlackHttpClient, instead?
        final PublishSlackClient slackClient = initializeClient(new App(appConfig).getClient());

        try {
            final PublishConfig config = parseOptions(botToken, context, session, flowFile, slackClient);
            final PublishStrategy strategy = strategyFor(config.getMode());
            if (strategy.willPostMessage() && isEmpty(config.getChannel())) {
                throw new RuntimeException("The channel must be specified when posting a message.");
            }
            if (strategy.willSendContent()) {
                try (InputStream stream = session.read(flowFile)) {
                    byte[] fileData = stream.readAllBytes();
                    strategy.setFileContent(config, fileData);
                }
                if (strategy.willUploadFile()) {
                    File upload = uploadFlowfile(strategy, config);
                    strategy.setUploadFile(config, upload);
                }
            }
            if (strategy.willPostMessage()) {
                strategy.addMessageAttachments(getLogger(), config, attachmentProperties);
                postMessage(strategy, config);
            }

            session.transfer(flowFile, REL_SUCCESS);

        } catch (IOException | SlackApiException | RuntimeException e) {
            getLogger().error("Failed to send message to Slack.", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private void postMessage(PublishStrategy strategy, PublishConfig config) throws IOException, SlackApiException {
        ChatPostMessageRequest request = strategy.buildPostMessageRequest(config);
        ChatPostMessageResponse response = config.getSlackClient().chatPostMessage(request);
        if (response.getWarning() != null) {
            getLogger().warn("Slack warning message: " + response.getWarning());
        }
        if (response.isOk()) {
            getLogger().debug("Slack response: ok, channel = " + response.getChannel() + ", ts = " + response.getTs());
        } else {
            String error = response.getError();
            getLogger().debug("Slack response: error = " + error);
            throw new RuntimeException("Slack error response: " + error);
        }
    }

    private File uploadFlowfile(PublishStrategy strategy, PublishConfig config) throws IOException, SlackApiException {
        FilesUploadV2Request request = strategy.buildFilesUploadV2Request(getLogger(), config);
        FilesUploadV2Response response = config.getSlackClient().filesUploadV2(request);
        if (response.getWarning() != null) {
            getLogger().warn("Slack warning message: " + response.getWarning());
        }
        if (!response.isOk()) {
            String error = response.getError();
            throw new RuntimeException("Slack error response: " + error);
        }
        return response.getFile();
    }

    public static String lookupChannelId(PublishSlackClient slackClient, String channel) throws SlackApiException, IOException {
        ConversationsListResponse response;
        String cursor = null;
        do {
            final ConversationsListRequest request = ConversationsListRequest.builder()
                    .cursor(cursor)
                    .excludeArchived(true)
                    .limit(500)
                    .build();

            response = slackClient.conversationsList(request);
            if (response.isOk()) {
                for (Conversation convo : response.getChannels()) {
                    if (channel.equals(convo.getName()) || channel.equals(convo.getId())) {
                        return convo.getId();
                    }
                }
                cursor = response.getResponseMetadata().getNextCursor();
            }
        } while (!isEmpty(cursor));

        final String errorMessage = ConsumeSlackUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
        throw new RuntimeException("Failed to determine Channel ID: " + errorMessage);
    }

    protected PublishStrategy strategyFor(String publishStrategy) {
        if (UPLOAD_ONLY.equals(publishStrategy)) {
            return new UploadOnlyStrategy();
        }
        if (MESSAGE_ONLY.equals(publishStrategy)) {
            return new MessageOnlyStrategy();
        }
        if (CONTENT_MESSAGE.equals(publishStrategy)) {
            return new ContentMessageStrategy();
        }
        if (UPLOAD_ATTACH.equals(publishStrategy)) {
            return new UploadAttachmentStrategy();
        }
        if (UPLOAD_LINK.equals(publishStrategy)) {
            return new UploadLinkStrategy();
        }
        throw new RuntimeException("Did not recognize the 'Publish Strategy': " + publishStrategy);
    }

    protected PublishConfig parseOptions(String botToken, ProcessContext context, ProcessSession session, FlowFile flowFile, PublishSlackClient slackClient) {
        final PublishConfig config = new PublishConfig();
        config.setContext(context);
        config.setSession(session);
        config.setFlowFile(flowFile);
        config.setSlackClient(slackClient);
        config.setMode(context.getProperty(PUBLISH_STRATEGY).getValue());
        config.setBotToken(botToken);
        config.setChannel(context.getProperty(CHANNEL).evaluateAttributeExpressions(flowFile).getValue());
        config.setUploadChannel(context.getProperty(UPLOAD_CHANNEL).evaluateAttributeExpressions(flowFile).getValue());
        config.setThreadTs(context.getProperty(THREAD_TS).evaluateAttributeExpressions(flowFile).getValue());
        config.setText(context.getProperty(TEXT).evaluateAttributeExpressions(flowFile).getValue());
        config.setInitComment(context.getProperty(INITIAL_COMMENT).evaluateAttributeExpressions(flowFile).getValue());
        config.setIconUrl(context.getProperty(ICON_URL).evaluateAttributeExpressions(flowFile).getValue());
        config.setIconEmoji(context.getProperty(ICON_EMOJI).evaluateAttributeExpressions(flowFile).getValue());
        config.setFileName(context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue());
        config.setFileTitle(context.getProperty(FILE_TITLE).evaluateAttributeExpressions(flowFile).getValue());
        return config;
    }

    public static String linkify(final String url, final String linkText) {
        if (isEmpty(url)) {
            return linkText;
        }
        if (isEmpty(linkText)) {
            return "<" + url + ">";
        }
        return "<" + url + "|" + linkText + ">";
    }

    public static String nullify(final String value) {
        return isEmpty(value) ? null : value;
    }

    protected static class EmojiValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (input.startsWith(":") && input.endsWith(":") && input.length() > 2) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

            return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                    .explanation("Must begin and end with a colon")
                    .build();
        }
    }


    protected static class DelegatingSlackClient implements PublishSlackClient {
        private final MethodsClient delegate;

        public DelegatingSlackClient(final MethodsClient delegate) {
            this.delegate = delegate;
        }

        @Override
        public ConversationsListResponse conversationsList(ConversationsListRequest request) throws SlackApiException, IOException {
            return delegate.conversationsList(request);
        }

        @Override
        public FilesUploadV2Response filesUploadV2(FilesUploadV2Request request) throws SlackFilesUploadV2Exception, SlackApiException, IOException {
            return delegate.filesUploadV2(request);
        }

        @Override
        public ChatPostMessageResponse chatPostMessage(ChatPostMessageRequest request) throws SlackApiException, IOException {
            return delegate.chatPostMessage(request);
        }
    }
}
