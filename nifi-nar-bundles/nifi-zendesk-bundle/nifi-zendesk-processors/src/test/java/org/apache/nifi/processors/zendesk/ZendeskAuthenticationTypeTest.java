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

package org.apache.nifi.processors.zendesk;

import org.apache.nifi.common.zendesk.ZendeskAuthenticationType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ZendeskAuthenticationTypeTest {

    private static Stream<Arguments> userNameAuthenticationTypeAndExpectedUserNameArguments() {
        return Stream.of(
            Arguments.of("user_1", ZendeskAuthenticationType.PASSWORD, "user_1"),
            Arguments.of("user_2", ZendeskAuthenticationType.TOKEN, "user_2/token")
        );
    }

    @ParameterizedTest
    @MethodSource("userNameAuthenticationTypeAndExpectedUserNameArguments")
    public void testUserNameIsEnrichedAccordingToAuthenticationType(String userName, ZendeskAuthenticationType authenticationType, String expectedUserName) {
        assertEquals(expectedUserName, authenticationType.enrichUserName(userName));
    }
}
