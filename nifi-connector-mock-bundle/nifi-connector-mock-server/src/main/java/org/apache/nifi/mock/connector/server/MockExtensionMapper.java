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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.HashMap;
import java.util.Map;

public class MockExtensionMapper {

    private final Map<String, String> processorMocks = new HashMap<>();
    private final Map<String, String> controllerServiceMocks = new HashMap<>();

    public void mockProcessor(final String processorType, final String mockProcessorClassName) {
        processorMocks.put(processorType, mockProcessorClassName);
    }

    public void mockControllerService(final String controllerServiceType, final String mockControllerServiceClassName) {
        controllerServiceMocks.put(controllerServiceType, mockControllerServiceClassName);
    }

    public void mapProcessor(final VersionedProcessor processor) {
        final String type = processor.getType();
        final String implementationClassName = processorMocks.get(type);
        if (implementationClassName == null) {
            return;
        }

        processor.setType(implementationClassName);
        processor.setBundle(new Bundle("org.apache.nifi.mock", implementationClassName, "1.0.0"));
    }

    public void mapControllerService(final VersionedControllerService controllerService) {
        final String type = controllerService.getType();
        final String implementationClassName = controllerServiceMocks.get(type);
        if (implementationClassName == null) {
            return;
        }

        controllerService.setType(implementationClassName);
        controllerService.setBundle(new Bundle("org.apache.nifi.mock", implementationClassName, "1.0.0"));
    }
}
