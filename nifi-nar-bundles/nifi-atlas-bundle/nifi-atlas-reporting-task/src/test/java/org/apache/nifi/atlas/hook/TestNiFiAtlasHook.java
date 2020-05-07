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
package org.apache.nifi.atlas.hook;

import org.apache.atlas.model.notification.HookNotification;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestNiFiAtlasHook {

    private NiFiAtlasHook hook;

    @Before
    public void setUp() {
        hook = new NiFiAtlasHook() {
            @Override
            NotificationSender createNotificationSender() {
                return mock(NotificationSender.class);
            }
        };
    }

    @Test
    public void messagesListShouldContainMessagesAfterAddMessage() {
        hook.addMessage(new HookNotification(HookNotification.HookNotificationType.ENTITY_CREATE, "nifi"));
        hook.addMessage(new HookNotification(HookNotification.HookNotificationType.ENTITY_PARTIAL_UPDATE, "nifi"));

        assertEquals(2, hook.getMessages().size());
    }

    @Test
    public void messagesListShouldBeCleanedUpAfterCommit() {
        hook.addMessage(new HookNotification(HookNotification.HookNotificationType.ENTITY_CREATE, "nifi"));
        hook.addMessage(new HookNotification(HookNotification.HookNotificationType.ENTITY_PARTIAL_UPDATE, "nifi"));

        hook.commitMessages();

        assertTrue(hook.getMessages().isEmpty());
    }
}
