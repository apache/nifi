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

package org.apache.nifi.stateless.basics;

import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ControllerServiceLifecycleIT extends StatelessSystemIT {

    @Override
    protected String getComponentEnableTimeout() {
        return "1 sec";
    }

    @Test
    public void testControllerServices() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();

        final VersionedControllerService sleepService = builder.createSimpleControllerService("StandardSleepService", "SleepService");
        sleepService.setProperties(Collections.singletonMap("@OnEnabled Sleep Time", "3 sec"));

        try {
            loadDataflow(builder.getFlowSnapshot());
        } catch (final IllegalStateException expected) {
            assertInstanceOf(TimeoutException.class, expected.getCause());
        }
    }

}
