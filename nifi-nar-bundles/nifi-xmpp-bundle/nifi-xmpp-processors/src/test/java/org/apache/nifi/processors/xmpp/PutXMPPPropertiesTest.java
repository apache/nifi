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
package org.apache.nifi.processors.xmpp;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class PutXMPPPropertiesTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PutXMPP.class);
        testRunner.setProperty(PutXMPP.HOSTNAME, "localhost");
        testRunner.setProperty(PutXMPP.PORT, "5222");
        testRunner.setProperty(PutXMPP.XMPP_DOMAIN, "domain");
        testRunner.setProperty(PutXMPP.USERNAME, "user");
        testRunner.setProperty(PutXMPP.PASSWORD, "password");
    }

    @Test
    public void whenAllRequiredPropertiesAreSet_processorIsValid() {
        testRunner.assertValid();
    }

    @Test
    public void whenHostnameIsNotSet_processorIsNotValid() {
        testRunner.removeProperty(PutXMPP.HOSTNAME);

        testRunner.assertNotValid();
    }

    @Test
    public void whenPortIsNotSet_processorIsNotValid() {
        testRunner.removeProperty(PutXMPP.PORT);

        testRunner.assertNotValid();
    }

    @Test
    public void whenXMPPDomainIsNotSet_processorIsNotValid() {
        testRunner.removeProperty(PutXMPP.XMPP_DOMAIN);

        testRunner.assertNotValid();
    }

    @Test
    public void whenUsernameIsNotSet_processorIsNotValid() {
        testRunner.removeProperty(PutXMPP.USERNAME);

        testRunner.assertNotValid();
    }

    @Test
    public void whenPasswordIsNotSet_processorIsNotValid() {
        testRunner.removeProperty(PutXMPP.PASSWORD);

        testRunner.assertNotValid();
    }

    @Test
    public void whenHostnameIsEmpty_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.HOSTNAME, "");

        testRunner.assertNotValid();
    }

    @Test
    public void whenPortIsEmpty_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.PORT, "");

        testRunner.assertNotValid();
    }

    @Test
    public void whenPortIsAString_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.PORT, "port");

        testRunner.assertNotValid();
    }

    @Test
    public void whenPortIsADecimalNumber_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.PORT, "52.22");

        testRunner.assertNotValid();
    }

    @Test
    public void whenPortIsTooSmall_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.PORT, "0");

        testRunner.assertNotValid();
    }

    @Test
    public void whenPortIsTooLarge_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.PORT, "65536");

        testRunner.assertNotValid();
    }

    @Test
    public void whenXMPPDomainIsEmpty_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.XMPP_DOMAIN, "");

        testRunner.assertNotValid();
    }

    @Test
    public void whenUsernameIsEmpty_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.USERNAME, "");

        testRunner.assertNotValid();
    }

    @Test
    public void whenPasswordIsEmpty_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.PASSWORD, "");

        testRunner.assertNotValid();
    }

    @Test
    public void whenResourceIsEmpty_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.RESOURCE, "");

        testRunner.assertNotValid();
    }

    @Test
    public void whenChatRoomIsEmpty_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.CHAT_ROOM, "");

        testRunner.assertNotValid();
    }

    @Test
    public void whenTargetUserIsEmpty_processorIsNotValid() {
        testRunner.setProperty(PutXMPP.TARGET_USER, "");

        testRunner.assertNotValid();
    }

}
