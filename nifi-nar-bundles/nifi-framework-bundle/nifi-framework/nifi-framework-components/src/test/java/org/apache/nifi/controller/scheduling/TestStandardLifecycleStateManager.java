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

package org.apache.nifi.controller.scheduling;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardLifecycleStateManager {
    private static final String COMPONENT_ID = "abc";


    @Test
    public void testGetOrRegisterWithoutReplace() {
        final StandardLifecycleStateManager manager = new StandardLifecycleStateManager();
        assertFalse(manager.getLifecycleState(COMPONENT_ID).isPresent());

        final LifecycleState registered = manager.getOrRegisterLifecycleState(COMPONENT_ID, false, false);
        assertNotNull(registered);

        final Optional<LifecycleState> stateOptional = manager.getLifecycleState(COMPONENT_ID);
        assertTrue(stateOptional.isPresent());
        assertSame(registered, stateOptional.get());
        assertSame(registered, manager.getOrRegisterLifecycleState(COMPONENT_ID, false, false));

        registered.terminate();
        assertSame(registered, manager.getOrRegisterLifecycleState(COMPONENT_ID, false, false));
    }

    @Test
    public void testGetOrRegisterReplaceTerminated() {
        final StandardLifecycleStateManager manager = new StandardLifecycleStateManager();

        final LifecycleState registered = manager.getOrRegisterLifecycleState(COMPONENT_ID, false, false);
        assertSame(registered, manager.getOrRegisterLifecycleState(COMPONENT_ID, true, false));

        registered.terminate();
        final LifecycleState replacement = manager.getOrRegisterLifecycleState(COMPONENT_ID, true, false);
        assertNotNull(replacement);
        assertNotEquals(registered, replacement);
    }

    @Test
    public void testGetOrRegisterReplaceUnscheduled() {
        final StandardLifecycleStateManager manager = new StandardLifecycleStateManager();

        final LifecycleState registered = manager.getOrRegisterLifecycleState(COMPONENT_ID, false, false);
        assertSame(registered, manager.getOrRegisterLifecycleState(COMPONENT_ID, true, false));

        registered.setScheduled(true);
        registered.terminate();
        assertSame(registered, manager.getOrRegisterLifecycleState(COMPONENT_ID, false, true));

        registered.setScheduled(false);
        final LifecycleState replacement = manager.getOrRegisterLifecycleState(COMPONENT_ID, false, true);
        assertNotNull(replacement);
        assertNotEquals(registered, replacement);
    }


}
