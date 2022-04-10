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
package org.apache.nifi.util.validator;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ControllerSetupValidatorTest {
    private Processor processor;
    private TestRunner testRunner;
    private ControllerServiceSetupValidator controllerServiceSetupValidator;

    @BeforeEach
    public void beforeEach() {
        controllerServiceSetupValidator = new ControllerServiceSetupValidator();
        processor = new NoOpProcessor();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setEnableControllServiceBestPracticeValidation(true);
    }

    @Test
    public void testFullyCorrectService() {
        assertDoesNotThrow(() -> testRunner.addControllerService("test", new CompleteService()));
    }

    @Test
    public void testCatchesDuplicateRelationships() {
        CompleteService service = new CompleteService();
        service.descriptors.add(new PropertyDescriptor
                .Builder()
                .fromPropertyDescriptor(service.descriptors.get(0))
                .build());
        assertThrows(AssertionFailedError.class, () -> testRunner.addControllerService("test", service));
    }
}

@Tags({"x", "y"})
@CapabilityDescription("Hello, world")
class CompleteService extends AbstractControllerService {
    public static final PropertyDescriptor TEST = new PropertyDescriptor.Builder()
            .name("test-test")
            .addValidator(Validator.VALID)
            .build();

    public List<PropertyDescriptor> descriptors;

    public CompleteService() {
        descriptors = new ArrayList<>();
        descriptors.add(TEST);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
}