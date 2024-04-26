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
package org.apache.nifi.web.security.logout;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestLogoutRequestManager {

    private LogoutRequestManager logoutRequestManager;

    @BeforeEach
    public void setup() {
        logoutRequestManager = new LogoutRequestManager();
    }

    @Test
    public void testLogoutSequence() {
        final String logoutRequestId = "logoutRequest1";
        final String userIdentity = "user1";

        // create the request
        final LogoutRequest logoutRequest = new LogoutRequest(logoutRequestId, userIdentity);
        logoutRequestManager.start(logoutRequest);

        // retrieve the request
        final LogoutRequest retrievedRequest = logoutRequestManager.get(logoutRequestId);
        assertNotNull(retrievedRequest);
        assertEquals(logoutRequestId, retrievedRequest.getRequestIdentifier());
        assertEquals(userIdentity, retrievedRequest.getMappedUserIdentity());

        // complete the request
        final LogoutRequest completedRequest = logoutRequestManager.complete(logoutRequestId);
        assertNotNull(completedRequest);
        assertEquals(logoutRequestId, completedRequest.getRequestIdentifier());
        assertEquals(userIdentity, completedRequest.getMappedUserIdentity());

        // verify request no long exists
        final LogoutRequest shouldNotExistRequest = logoutRequestManager.get(logoutRequestId);
        assertNull(shouldNotExistRequest);
    }

    @Test
    public void testCompleteLogoutWhenDoesNotExist() {
        final LogoutRequest shouldNotExistRequest = logoutRequestManager.complete("does-not-exist");
        assertNull(shouldNotExistRequest);
    }
}
