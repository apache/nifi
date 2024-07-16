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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.reactions.ReactionsGetRequest;
import com.slack.api.methods.response.reactions.ReactionsGetResponse;
import com.slack.api.model.Reaction;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.slack.util.RateLimit;
import org.apache.nifi.processors.slack.util.SlackResponseUtil;

import static com.jayway.jsonpath.Option.ALWAYS_RETURN_LIST;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.flowfile.attributes.CoreAttributes.FILENAME;
import static org.apache.nifi.flowfile.attributes.CoreAttributes.MIME_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({
  @ReadsAttribute(attribute = GetSlackReaction.ATTR_CHANNEL_ID, description = "If set, the ID of the channel where the message was sent is taken from this attribute."),
  @ReadsAttribute(attribute = GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, description = "If set, the message timestamp is taken from this attribute.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "slack.reaction.<emoji name>", description = "The name of the reaction (emoji) and the reaction count that was provided for the message."),
        @WritesAttribute(attribute = GetSlackReaction.ATTR_ERROR_MESSAGE, description = "The error message on fetching reactions."),
        @WritesAttribute(attribute = GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, description = "The ID of the Slack Channel where the message the reactions are fetched from."),
        @WritesAttribute(attribute = GetSlackReaction.ATTR_CHANNEL_ID, description = "The timestamp of the Slack message the reactions are fetched for."),
        @WritesAttribute(attribute = GetSlackReaction.ATTR_REACTION_POLLING_SECONDS, description = "The time in seconds the processor waited for reactions.")
})
@SeeAlso({ListenSlack.class, ConsumeSlack.class, PublishSlack.class})
@Tags({"slack", "conversation", "reactions.get", "social media", "emoji"})
@CapabilityDescription("Retrieves reactions for (a) given message(s). The reactions are written as Flow File attributes." +
        "ConsumeSlack, ListenSlack or PublishSlack processor should be used before GetSlackReaction.")
@DefaultSettings(penaltyDuration = "5 min")
@UseCase(
        description = "Fetch the reactions for a specific message that was earlier published by PublishSlack to a channel",
        configuration = """
            Set "Access Token" to the value of your Slack OAuth Access Token.
            Set 'Message Identifier Strategy' to 'Attributes' (channel ID and message timestamp will be taken from attributes set by PublishSlack)
            Set "Release Per Reaction" to 'true' if you wait for the first reaction(s) only, in case want to collect all reactions in a given waiting period,
            set this property to 'false'.
            Set "Wait Period" to the time period the processor will continue to search for reactions for the specified message (unless releasing per reaction)
            """
)
@UseCase(
        description = "Fetch the reactions for messages fetched by ConsumeSlack/ListenSlack",
        configuration = """
            Set "Access Token" to the value of your Slack OAuth Access Token.
            Set 'Message Identifier Strategy' to 'JSON Path' (channel ID and message timestamp will be taken from the JSON output of ConsumeSlack/ListenSlack)
            You can leave "Channel ID JSON Path", "Message Timestamp JSON Path" and "Message Text JSON Path" on default values.
            Set "Release Per Reaction" to 'true' if you wait for the first reaction(s) only, in case want to collect all reactions in a given waiting period,
            set this property to 'false'.
            Set "Wait Period" to the time period the processor will continue to search for reactions for the specified message (unless releasing per reaction)
            """
)
public class GetSlackReaction extends AbstractProcessor {
    public static final String ATTR_MESSAGE_TIMESTAMP = "slack.ts";
    public static final String ATTR_CHANNEL_ID = "slack.channel.id";
    public static final String ATTR_ERROR_MESSAGE = "error.message";
    public static final String ATTR_FIRST_REACTION_CHECK_TIMESTAMP = "first.reaction.check.ts";
    public static final String ATTR_LAST_REACTION_CHECK_TIMESTAMP = "last.reaction.check.ts";
    public static final String ATTR_REACTION_POLLING_SECONDS = "reaction.polling.seconds";
    public static final String ATTR_SLACK_MESSAGE_COUNT = "slack.message.count";
    public static final int MIN_REACTION_POLLING_FREQUENCY_SECONDS = 10;


    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .description("OAuth Access Token used for authenticating/authorizing the Slack request sent by NiFi. This may be either a User Token or a Bot Token. " +
                    "It must be granted the reactions:read scope.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    static final PropertyDescriptor MESSAGE_IDENTIFIER_STRATEGY = new PropertyDescriptor.Builder()
            .name("Message Identifier Strategy")
            .description("Specifies the strategy to obtain the message timestamp and channel id from incoming Flow File. "
             + "In case of '" + MessageIdentifierStrategy.ATTRIBUTES.getValue() + "' the 'slack.channel.id' and 'slack.ts' Flow File attributes will be used "
             + "and in case of '" + MessageIdentifierStrategy.JSON_PATH.getValue() + "' additional JSON path values needs to be specified to fetch channel id "
             + "and message timestamp from incoming JSON content.")
            .required(true)
            .allowableValues(MessageIdentifierStrategy.class)
            .defaultValue(MessageIdentifierStrategy.ATTRIBUTES.getValue())
            .build();

    static final PropertyDescriptor CHANNEL_ID_JSON_PATH = new PropertyDescriptor.Builder()
            .name("Channel ID JSON Path")
            .description("The JSON Path which identifies the channel ID in the incoming JSON.")
            .required(true)
            .defaultValue("$.channel")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(MESSAGE_IDENTIFIER_STRATEGY, MessageIdentifierStrategy.JSON_PATH)
            .build();

    static final PropertyDescriptor MESSAGE_TIMESTAMP_JSON_PATH = new PropertyDescriptor.Builder()
            .name("Message Timestamp JSON Path")
            .description("The JSON Path which identifies the message timestamp in the incoming JSON.")
            .required(true)
            .defaultValue("$.ts")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(MESSAGE_IDENTIFIER_STRATEGY, MessageIdentifierStrategy.JSON_PATH)
            .build();

    static final PropertyDescriptor MESSAGE_TEXT_JSON_PATH = new PropertyDescriptor.Builder()
            .name("Message Text JSON Path")
            .description("The JSON Path which identifies the message text in the incoming JSON.")
            .required(true)
            .defaultValue("$.text")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(MESSAGE_IDENTIFIER_STRATEGY, MessageIdentifierStrategy.JSON_PATH)
            .build();

    static final PropertyDescriptor RELEASE_PER_REACTION = new PropertyDescriptor.Builder()
            .name("Release Per Reaction")
            .description("If true the Flow File will be released each time a reaction is found for the specified message.")
            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    static final PropertyDescriptor WAIT_PERIOD = new PropertyDescriptor.Builder()
            .name("Wait Period")
            .description("The period of time a processor will continue to search for reactions for the specified message unless releasing per reaction. "
             + "Note: The elapsed time is calculated by considering the Slack message timestamp as start time.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 hour")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages are routed to this relationship when reactions are found and either the wait period has elapsed or releasing per reaction is configured.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Messages are routed to this relationship if an unrecoverable error occurred when fetching reactions, e.g. the message was not found.")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original Flow File is routed to this relationship.")
            .build();

    static final Relationship REL_WAIT = new Relationship.Builder()
            .name("wait")
            .description("Further reactions may occur and the waiting period has not elapsed. Consider self-looping.")
            .build();

    static final Relationship REL_NO_REACTION = new Relationship.Builder()
            .name("no_reaction")
            .description("Messages are routed to this relationship if no reaction arrived for the message in the given waiting period.")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            ACCESS_TOKEN,
            MESSAGE_IDENTIFIER_STRATEGY,
            MESSAGE_TIMESTAMP_JSON_PATH,
            CHANNEL_ID_JSON_PATH,
            MESSAGE_TEXT_JSON_PATH,
            WAIT_PERIOD,
            RELEASE_PER_REACTION);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS,
            REL_WAIT,
            REL_NO_REACTION,
            REL_FAILURE,
            REL_ORIGINAL);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private RateLimit rateLimit;
    private volatile App slackApp;
    private MethodsClient client;

    @OnScheduled
    public void setup(final ProcessContext context) {
        rateLimit = new RateLimit(getLogger());
        slackApp = createSlackApp(context);
        client = initializeClient(slackApp);
    }

    @OnStopped
    public void shutdown() {
        if (slackApp != null) {
            slackApp.stop();
            slackApp = null;
        }
        rateLimit = null;
    }

    private App createSlackApp(final ProcessContext context) {
        final String botToken = context.getProperty(ACCESS_TOKEN).getValue();
        final AppConfig appConfig = AppConfig.builder()
                .singleTeamBotToken(botToken)
                .build();

        return new App(appConfig);
    }

    protected MethodsClient initializeClient(final App slackApp) {
        slackApp.start();
        return slackApp.client();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (rateLimit.isLimitReached()) {
            getLogger().debug("Will not fetch reactions for the message because rate limit has been reached");
            context.yield();
            return;
        }

        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final MessageIdentifierStrategy messageIdentifierStrategy = MessageIdentifierStrategy.forValue(
                context.getProperty(MESSAGE_IDENTIFIER_STRATEGY).getValue());
        final String channelId = flowFile.getAttribute(ATTR_CHANNEL_ID);
        final String messageTimestamp = flowFile.getAttribute(ATTR_MESSAGE_TIMESTAMP);

        if (channelId != null && messageTimestamp != null) {
            if (flowFile.getAttribute(ATTR_LAST_REACTION_CHECK_TIMESTAMP) == null) {
                //first check for reactions of a message
                FlowFile flowFileForMessage = session.clone(flowFile);
                session.transfer(flowFile, REL_ORIGINAL);
                handleReactions(context, session, flowFileForMessage, messageTimestamp, channelId);
            } else {
                //reactions has been checked for this Flow File, it came from WAIT relationship
                handleReactions(context, session, flowFile, messageTimestamp, channelId);
            }
        } else if (MessageIdentifierStrategy.JSON_PATH == messageIdentifierStrategy) {
            final String messageTimestampJsonPath = context.getProperty(MESSAGE_TIMESTAMP_JSON_PATH).getValue();
            final String channelIdJsonPath = context.getProperty(CHANNEL_ID_JSON_PATH).getValue();
            final String messageTextJsonPath = context.getProperty(MESSAGE_TEXT_JSON_PATH).getValue();

            try (final InputStream rawIn = session.read(flowFile)) {
                final Configuration pathConfiguration = Configuration.builder().options(ALWAYS_RETURN_LIST).build();

                final DocumentContext parsedJsonArray = JsonPath.using(pathConfiguration).parse(rawIn);
                final List<LinkedHashMap<String, String>> jsonArray = parsedJsonArray.read("$.*");

                for (int i = 0; i < jsonArray.size(); i++) {
                    final LinkedHashMap<String, String> json = jsonArray.get(i);
                    String channelIdOfMessage;
                    String timestampOfMessage;
                    String messageText;

                    try {
                        final DocumentContext parsedJson = JsonPath.parse(json);
                        channelIdOfMessage = parseValue(parsedJson, channelIdJsonPath);
                        timestampOfMessage = parseValue(parsedJson, messageTimestampJsonPath);
                        messageText = parseValue(parsedJson, messageTextJsonPath);
                    } catch (PathNotFoundException e) {
                        routeToFailure(session, flowFile, e.getMessage());
                        return;
                    }

                    FlowFile flowFileForMessage = session.create(flowFile);
                    flowFileForMessage = session.write(flowFileForMessage, (outputStream) -> outputStream.write(messageText.getBytes(UTF_8)));
                    flowFileForMessage = session.putAttribute(flowFileForMessage, ATTR_MESSAGE_TIMESTAMP, timestampOfMessage);
                    flowFileForMessage = session.putAttribute(flowFileForMessage, ATTR_CHANNEL_ID, channelIdOfMessage);
                    flowFileForMessage = session.putAttribute(flowFileForMessage, FILENAME.key(), flowFile.getAttribute(FILENAME.key()) + "_" + i);

                    flowFileForMessage = session.removeAttribute(flowFileForMessage, ATTR_SLACK_MESSAGE_COUNT);
                    flowFileForMessage = session.removeAttribute(flowFileForMessage, MIME_TYPE.key());

                    handleReactions(context, session, flowFileForMessage, timestampOfMessage, channelIdOfMessage);
                }
                session.transfer(flowFile, REL_ORIGINAL);
            } catch (Exception e) {
                getLogger().error("Exception occurred while fetching reactions", e);
                routeToFailure(session, flowFile, e.getMessage());
            }
        } else {
            final String errorMessage = format("Required attributes are missing from the Flow File while [%s] is set to [%s]",
                    MESSAGE_IDENTIFIER_STRATEGY.getDisplayName(), MessageIdentifierStrategy.ATTRIBUTES.getValue());
            getLogger().warn(errorMessage);
            routeToFailure(session, flowFile, errorMessage);
        }
    }

    private String parseValue(final DocumentContext json, final String jsonPath) {
        try {
           return json.read(jsonPath);
        } catch (PathNotFoundException e) {
            getLogger().warn("Failed to find the provided JSON path [{}] in the incoming JSON", jsonPath);
            throw e;
        }
    }

    private void handleReactions(final ProcessContext context, final ProcessSession session, FlowFile flowFile,
                                 final String messageTimestamp, final String channelId) {
        try {
            final String botToken = context.getProperty(ACCESS_TOKEN).getValue();
            final boolean releasePerReaction = context.getProperty(RELEASE_PER_REACTION).asBoolean();
            final long waitPeriodInMicroSec = context.getProperty(WAIT_PERIOD).asTimePeriod(TimeUnit.MICROSECONDS);

            final long elapsedTimeMicroseconds = getElapsedTimeMicroseconds(messageTimestamp);

            if (isReactionCheckTooFrequent(flowFile)) {
                session.transfer(session.penalize(flowFile), REL_WAIT);
                getLogger().warn("Reactions are checked too frequently, minimum reaction polling frequency is {} sec. "
                        + "Penalizing the Flow File and sending it to [{}] relationship.", MIN_REACTION_POLLING_FREQUENCY_SECONDS, REL_WAIT);
                return;
            }

            final ReactionsGetResponse results = fetchReactions(channelId, botToken, messageTimestamp);

            final String currentTimeInUTC = String.valueOf(Instant.now());

            if (flowFile.getAttribute(ATTR_FIRST_REACTION_CHECK_TIMESTAMP) == null) {
                flowFile = session.putAttribute(flowFile, ATTR_FIRST_REACTION_CHECK_TIMESTAMP, currentTimeInUTC);
            }

            flowFile = session.putAttribute(flowFile, ATTR_LAST_REACTION_CHECK_TIMESTAMP, currentTimeInUTC);

            final ReactionsGetResponse.Message message = results.getMessage();

            if (message == null) {
                throw new ProcessException(format("Failed to fetch message [%s] in channel [%s]", messageTimestamp, channelId));
            }

            final List<Reaction> reactions = results.getMessage().getReactions();

            if ((elapsedTimeMicroseconds >= waitPeriodInMicroSec)) {
                if (!reactionFound(reactions)) {
                    session.transfer(adjustAttributes(session, flowFile), REL_NO_REACTION);
                } else {
                    createSuccessOutput(session, flowFile, reactions);
                }
            } else {
                if (reactionFound(reactions) && releasePerReaction) {
                    createSuccessOutput(session, flowFile, reactions);
                } else {
                    session.transfer(session.penalize(flowFile), REL_WAIT);
                }
            }
        } catch (Exception e) {
           if (SlackResponseUtil.isRateLimited(e)) {
                session.rollback();
                getLogger().warn("Slack indicated that the Rate Limit has been exceeded when attempting to fetch reactions for message [{}] in channel [{}]",
                        messageTimestamp, channelId);
                yieldOnException(e, context);
           } else {
               routeToFailure(session, flowFile, e.getMessage());
           }
        }
    }

    private void routeToFailure(final ProcessSession session, FlowFile flowFile, final String errorMessage) {
        flowFile = session.putAttribute(flowFile, ATTR_ERROR_MESSAGE, errorMessage);
        session.transfer(session.penalize(flowFile), REL_FAILURE);
    }

    private long getElapsedTimeMicroseconds(String messageTimestamp) {
        final long messageTimeStampMicroseconds = Long.parseLong(messageTimestamp.replace(".", ""));
        return Instant.now().toEpochMilli() * 1000 - messageTimeStampMicroseconds;
    }

    private boolean isReactionCheckTooFrequent(final FlowFile flowFile) {
        final String lastCheckTimestamp = flowFile.getAttribute(ATTR_LAST_REACTION_CHECK_TIMESTAMP);

        if (lastCheckTimestamp != null) {
            final Instant lastCheckInstant = Instant.parse(lastCheckTimestamp);
            return Instant.now().minusSeconds(MIN_REACTION_POLLING_FREQUENCY_SECONDS).compareTo(lastCheckInstant) < 1;
        }

        return false;
    }

    private FlowFile adjustAttributes(final ProcessSession session, FlowFile flowFile) {
        final String firstReactionCheckTs = flowFile.getAttribute(ATTR_FIRST_REACTION_CHECK_TIMESTAMP);
        final String lastReactionCheckTs = flowFile.getAttribute(ATTR_LAST_REACTION_CHECK_TIMESTAMP);

        if (firstReactionCheckTs != null && lastReactionCheckTs != null) {
            final Instant firstReactionCheck = Instant.parse(firstReactionCheckTs);
            final Instant lastReactionCheck = Instant.parse(lastReactionCheckTs);
            flowFile = session.putAttribute(flowFile, ATTR_REACTION_POLLING_SECONDS,
                    String.valueOf(ChronoUnit.SECONDS.between(firstReactionCheck, lastReactionCheck)));
            session.removeAttribute(flowFile, ATTR_FIRST_REACTION_CHECK_TIMESTAMP);
            session.removeAttribute(flowFile, ATTR_LAST_REACTION_CHECK_TIMESTAMP);
        }

        return flowFile;
    }

    private ReactionsGetResponse fetchReactions(final String channelId, final String accessToken, final String threadTimestamp)
            throws SlackApiException, IOException {
        final ReactionsGetRequest request = ReactionsGetRequest.builder()
                .channel(channelId)
                .full(true)
                .token(accessToken)
                .timestamp(threadTimestamp)
                .build();

        return client.reactionsGet(request);
    }

    private void createSuccessOutput(final ProcessSession session, FlowFile flowFile, final List<Reaction> reactions)  {
        for (final Reaction reaction : reactions) {
            flowFile = session.putAttribute(flowFile, "slack.reaction." + reaction.getName(), String.valueOf(reaction.getCount()));
        }
        session.transfer(adjustAttributes(session, flowFile), REL_SUCCESS);
    }

    private void yieldOnException(final Throwable t, final ProcessContext context) {
        final int retryAfterSeconds = SlackResponseUtil.getRetryAfterSeconds(t);
        rateLimit.retryAfter(Duration.ofSeconds(retryAfterSeconds));
        context.yield();
    }

    private boolean reactionFound(final List<Reaction> reactions) {
        return reactions != null && !reactions.isEmpty();
    }

    enum MessageIdentifierStrategy implements DescribedValue {
        ATTRIBUTES( "Attributes", "Flow File attributes are used to find out the Slack message timestamp and channel ID."),
        JSON_PATH("JSON Path", "JSON path is used to find out the Slack message timestamp and channel ID from the incoming JSON Flow File content.");

        private static final Map<String, MessageIdentifierStrategy> ENUM_MAP = new HashMap<>();

        static {
            for (MessageIdentifierStrategy strategy : MessageIdentifierStrategy.values()) {
                ENUM_MAP.put(strategy.getValue(), strategy);
            }
        }

        private final String value;
        private final String description;

        MessageIdentifierStrategy(final String value, String description) {
            this.value = value;
            this.description = description;
        }

        public static MessageIdentifierStrategy forValue(String value) {
            return ENUM_MAP.get(value);
        }

        @Override
        public String getValue() {
            return this.value;
        }

        @Override
        public String getDisplayName() {
            return this.value;
        }

        @Override
        public String getDescription() {
            return this.description;
        }
    }
}
