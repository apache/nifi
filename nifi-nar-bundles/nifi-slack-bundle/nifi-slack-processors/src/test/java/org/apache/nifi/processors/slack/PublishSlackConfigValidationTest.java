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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PublishSlackConfigValidationTest {

    private TestRunner testRunner;

    @BeforeEach
    public void setup() {
        testRunner = TestRunners.newTestRunner(PublishSlack.class);
    }

    @Test
    public void validationShouldPassIfTheConfigIsFine() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.THREAD_TS, "thread-timestamp");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");
        testRunner.setProperty(PublishSlack.ICON_URL, "http://slack.com/emojis/happy.gif");
        testRunner.setProperty(PublishSlack.ICON_EMOJI, ":happy:");

        testRunner.assertValid();
    }

    @Test
    public void validationShouldPassIfMinimalConfigForMessageOnly() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);

        testRunner.assertValid();
    }

    @Test
    public void validationShouldFailIfAccessTokenIsNotGiven() {
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfAccessTokenIsEmptyString() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfChannelIsNotGiven() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfChannelIsEmptyString() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfThreadTsIsEmptyString() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.THREAD_TS, "");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfIconEmojiInvalid() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.ICON_EMOJI, "sadface");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfIconUrlInvalid() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.ICON_URL, "not-url");
        testRunner.setProperty(PublishSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfTextIsNotGivenAndNoAttachmentSpecifiedForMessageOnly() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldPassWithoutTextForUploadOnly() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_ONLY);

        testRunner.assertValid();
    }

    @Test
    public void validationShouldPassIfTextIsNotGivenButAttachmentSpecified() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.assertValid();
    }

    @Test
    public void validationShouldPassIfTextIsNotGivenButUploadAttachChosen() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        // UPLOAD_ATTACH is the default
        // testRunner.setProperty(PublishSlack.PUBLISH_MODE, PublishSlack.STRATEGY_UPLOAD_ATTACH);

        testRunner.assertValid();
    }

    @Test
    public void validationShouldPassIfTextIsNotGivenButUploadLinkChosen() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_LINK);

        testRunner.assertValid();
    }

    @Test
    public void validationShouldPassIfTextIsNotGivenButContentFlowfileChosen() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_CONTENT_MESSAGE);

        testRunner.assertValid();
    }

    @Test
    public void validationShouldPassIfChannelIsNotGivenButUploadOnlyChosen() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_ONLY);

        testRunner.assertValid();
    }

    @Test
    public void validationShouldFailForUploadOnlyIfInitialCommentIsEmptyString() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, "my-access-token");
        testRunner.setProperty(PublishSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PublishSlack.INITIAL_COMMENT, "");
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_ONLY);

        testRunner.assertNotValid();
    }
}
