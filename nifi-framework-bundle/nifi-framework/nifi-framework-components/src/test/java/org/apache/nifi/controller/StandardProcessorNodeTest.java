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
package org.apache.nifi.controller;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.components.validation.VerifiableComponentFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.NoOpProcessor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class StandardProcessorNodeTest {

    @Test
    void testYieldExpiration() {
        final ProcessScheduler processScheduler = mock(ProcessScheduler.class);
        final Processor processor = new NoOpProcessor();
        final LoggableComponent<Processor> loggableProcessor = new LoggableComponent<>(processor, BundleCoordinate.UNKNOWN_COORDINATE, null);
        final StandardProcessorNode processorNode = new StandardProcessorNode(loggableProcessor, "processor",
                mock(ValidationContextFactory.class), processScheduler, mock(ControllerServiceProvider.class), mock(ReloadComponent.class),
                mock(VerifiableComponentFactory.class), mock(ExtensionManager.class), mock(ValidationTrigger.class));

        processorNode.yield(0L, TimeUnit.MILLISECONDS);
        assertEquals(0L, processorNode.getYieldExpiration());

        processorNode.yield(1L, TimeUnit.DAYS);
        final long expiration = processorNode.getYieldExpiration();
        assertTrue(expiration > System.currentTimeMillis());

        processorNode.yield(1L, TimeUnit.SECONDS);
        assertEquals(expiration, processorNode.getYieldExpiration());
    }
}
