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
import org.junit.Before;
import org.junit.Test;

public class PostSlackConfigValidationTest {

    private TestRunner testRunner;

    @Before
    public void setup() {
        testRunner = TestRunners.newTestRunner(PostSlack.class);
    }

    @Test
    public void validationShouldPassIfTheConfigIsFine() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertValid();
    }

    @Test
    public void validationShouldFailIfPostMessageUrlIsEmptyString() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, "");
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfPostMessageUrlIsNotValid() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, "not-url");
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfFileUploadUrlIsEmptyString() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, "");
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfFileUploadUrlIsNotValid() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, "not-url");
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfAccessTokenIsNotGiven() {
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfAccessTokenIsEmptyString() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfChannelIsNotGiven() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfChannelIsEmptyString() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfTextIsNotGivenAndNoAttachmentSpecifiedNorFileUploadChosen() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldPassIfTextIsNotGivenButAttachmentSpecified() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.assertValid();
    }

    @Test
    public void validationShouldPassIfTextIsNotGivenButFileUploadChosen() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);

        testRunner.assertValid();
    }

    @Test
    public void validationShouldFailIfTextIsEmptyString() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "");
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);

        testRunner.assertNotValid();
    }
}
