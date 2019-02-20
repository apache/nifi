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
    public void validationShouldFailIfPostMessageUrlIsEmpty() {
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
    public void validationShouldFailIfFileUploadUrlIsEmpty() {
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
    public void validationShouldFailIfAccessTokenIsEmpty() {
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
    public void validationShouldFailIfChannelIsEmpty() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfTextIsNotGiven() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");

        testRunner.assertNotValid();
    }

    @Test
    public void validationShouldFailIfTextIsEmpty() {
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "");

        testRunner.assertNotValid();
    }
}
