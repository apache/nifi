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
import com.slack.api.methods.request.reactions.ReactionsGetRequest;
import com.slack.api.methods.response.reactions.ReactionsGetResponse;
import com.slack.api.model.Reaction;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.slack.util.RateLimit;
import org.apache.nifi.processors.slack.util.SlackResponseUtil;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = GetSlackReaction.ATTR_CHANNEL_ID, description = "The ID of the Slack Channel from which the messages were retrieved"),
        @WritesAttribute(attribute = GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, description = "The timestamp of the Slack message the reactions are fetched for."),
        @WritesAttribute(attribute = "slack.reaction.<emoji name>", description = "The name of the emoji and the reaction count that was provided for the message."),
        @WritesAttribute(attribute = GetSlackReaction.ATTR_WAIT_TIME, description = "The total number of minutes waited while the reactions were captured."),
        @WritesAttribute(attribute = GetSlackReaction.ATTR_ERROR_MESSAGE, description = "The error message on fetching reactions.")
})
@SeeAlso({ListenSlack.class, ConsumeSlack.class, PublishSlack.class})
@Tags({"slack", "conversation", "reactions.get", "social media", "emoji"})
@CapabilityDescription("Retrieves reactions for a given message. The reactions are written as attributes.")
@DefaultSettings(penaltyDuration = "5 min")
public class GetSlackReaction extends AbstractProcessor {
    public static final String ATTR_WAIT_TIME = "minutes.waited";
    public static final String ATTR_ERROR_MESSAGE = "error.message";
    public static final String ATTR_MESSAGE_TIMESTAMP = "slack.message.timestamp";
    public static final String ATTR_CHANNEL_ID = "slack.channel.id";
    public static final int PENALTY_MINUTES = 5;

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .description("OAuth Access Token used for authenticating/authorizing the Slack request sent by NiFi. This may be either a User Token or a Bot Token. " +
                    "It must be granted the reactions:read scope and channels:history, groups:history, im:history or mpim:history, depending on the type of conversation being used.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    static final PropertyDescriptor CHANNEL_ID = new PropertyDescriptor.Builder()
            .name("Channel ID")
            .description("The ID of the channel.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor THREAD_TIMESTAMP = new PropertyDescriptor.Builder()
            .name("Message Timestamp")
            .description("Slack message's timestamp the reactions are fetched for.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor RELEASE_IF_ONE_REACTION = new PropertyDescriptor.Builder()
            .name("Release If One Reaction Arrived")
            .description("It is possible to wait for the first reaction or wait for the specified amount of time to fetch all possible reactions.")
            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    static final PropertyDescriptor WAIT_MONITOR_WINDOW = new PropertyDescriptor.Builder()
            .name("Wait Monitor Window")
            .description("Processor will periodically poll reactions for the given time duration if no reaction arrived yet or " +
                    RELEASE_IF_ONE_REACTION.getDisplayName() + " is set to false.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 hour")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to this relationship if fetching reactions were successful")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship if an error occurred when fetching reactions")
            .build();

    static final Relationship REL_WAIT = new Relationship.Builder()
            .name("wait")
            .description("Self loop relationship which is used when waiting for further reactions")
            .build();

    static final Relationship REL_NO_REACTION = new Relationship.Builder()
            .name("no_reaction")
            .description("FlowFiles are routed to this relationship if no reaction arrived for the message in the given timeframe")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(CHANNEL_ID,
            THREAD_TIMESTAMP,
            ACCESS_TOKEN,
            WAIT_MONITOR_WINDOW,
            RELEASE_IF_ONE_REACTION);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS,
            REL_WAIT,
            REL_NO_REACTION,
            REL_FAILURE);

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

    public RateLimit getRateLimit() {
        return rateLimit;
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
            getLogger().debug("Will not fetch reactions from Slack because rate limit has been reached");
            context.yield();
            return;
        }

        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final String botToken = context.getProperty(ACCESS_TOKEN).getValue();
        final String channelId = context.getProperty(CHANNEL_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String threadTimestamp = context.getProperty(THREAD_TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
        final boolean releaseIfOneReaction = context.getProperty(RELEASE_IF_ONE_REACTION).asBoolean();
        final long waitMonitorWindow = context.getProperty(WAIT_MONITOR_WINDOW).asTimePeriod(TimeUnit.MINUTES);
        int timeWaited = Integer.parseInt(Optional.ofNullable(flowFile.getAttribute(ATTR_WAIT_TIME)).orElse("0"));

        try {
            final ReactionsGetResponse results = fetchReactions(channelId, botToken, threadTimestamp);
            final ReactionsGetResponse.Message message = results.getMessage();

            if (message == null) {
                throw new ProcessException(String.format("Failed to fetch message [%s] in channel [%s]", threadTimestamp, channelId));
            }

            final List<Reaction> reactions = results.getMessage().getReactions();

            if ((timeWaited >= waitMonitorWindow) || (waitMonitorWindow < PENALTY_MINUTES)) {
                if (!reactionFound(reactions)) {
                    flowFile = session.putAllAttributes(flowFile, createAttributeMap(channelId, threadTimestamp));
                    session.transfer(flowFile, REL_NO_REACTION);
                } else {
                    createSuccessOutput(session, flowFile, channelId, threadTimestamp, reactions);
                }
            } else {
                if (reactionFound(reactions) && releaseIfOneReaction) {
                    createSuccessOutput(session, flowFile, channelId, threadTimestamp, reactions);
                } else {
                    flowFile = session.putAttribute(flowFile, ATTR_WAIT_TIME, String.valueOf(timeWaited + PENALTY_MINUTES));
                    session.transfer(session.penalize(flowFile), REL_WAIT);
                }
            }
        } catch (Exception e) {
           if (SlackResponseUtil.isRateLimited(e)) {
                    session.rollback();
                    getLogger().warn("Slack indicated that the Rate Limit has been exceeded when attempting to fetch reactions for message [{}] in channel [{}]", threadTimestamp, channelId);
                    yieldOnException(e, context);
                } else {
                    Map<String, String> attributeMap = createAttributeMap(channelId, threadTimestamp);
                    attributeMap.put(ATTR_ERROR_MESSAGE, e.getMessage());
                    final FlowFile outFlowFile = session.putAllAttributes(flowFile, attributeMap);
                    session.transfer(session.penalize(outFlowFile), REL_FAILURE);
                }
        }
    }

    private ReactionsGetResponse fetchReactions(final String channelId, final String accessToken, final String threadTimestamp) throws SlackApiException, IOException {
        ReactionsGetRequest request = ReactionsGetRequest.builder()
                .channel(channelId)
                .full(true)
                .token(accessToken)
                .timestamp(threadTimestamp)
                .build();

        return client.reactionsGet(request);
    }

    private void createSuccessOutput(final ProcessSession session, FlowFile flowFile, String channelId, String threadTimestamp, final List<Reaction> reactions)  {
        for (Reaction reaction : reactions) {
            flowFile = session.putAttribute(flowFile, "slack.reaction." + reaction.getName(), String.valueOf(reaction.getCount()));
        }

        flowFile = session.putAllAttributes(flowFile, createAttributeMap(channelId, threadTimestamp));
        session.transfer(flowFile, REL_SUCCESS);
    }

    private Map<String, String> createAttributeMap(String channelId, String threadTimestamp) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_CHANNEL_ID, channelId);
        attributes.put(ATTR_MESSAGE_TIMESTAMP, threadTimestamp);
        return attributes;
    }

    private void yieldOnException(final Throwable t, final ProcessContext context) {
        final int retryAfterSeconds = SlackResponseUtil.getRetryAfterSeconds(t);
        rateLimit.retryAfter(Duration.ofSeconds(retryAfterSeconds));
        context.yield();
    }

    private static boolean reactionFound(final List<Reaction> reactions) {
        return reactions != null && !reactions.isEmpty();
    }
}
