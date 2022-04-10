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
import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.util.validator.BaseSetupValidator.MISSING_CAPABILITY_DESCRIPTION;
import static org.apache.nifi.util.validator.BaseSetupValidator.MISSING_TAGS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessorSetupValidatorTest {
    private ProcessorSetupValidator processorSetupValidator;
    private TestRunner runner;
    private Processor processor;

    @BeforeEach
    public void beforeEach() {
        processorSetupValidator = new ProcessorSetupValidator();
    }

    private void init(AbstractConfigurableComponent processor) {
        this.processor = (Processor) processor;
        runner = TestRunners.newTestRunner(this.processor);
        runner.assertValid();
        processorSetupValidator.setComponent(processor);
    }

    private void testMissingAnnotation(AbstractConfigurableComponent processor, String comparison) {
        init(processor);
        AssertionFailedError error = assertThrows(AssertionFailedError.class,
                () -> processorSetupValidator.validateBasicAnnotations());
        assertTrue(error.getMessage().contains(comparison));
    }

    @Test
    public void testThrowsErrorOnMissingTags() {
        testMissingAnnotation(new OnlyMissingTags(), MISSING_TAGS);
    }

    @Test
    public void testThrowsErrorOnCapabilityDescription() {
        testMissingAnnotation(new OnlyMissingCapabilitiyDescription(), MISSING_CAPABILITY_DESCRIPTION);
    }

    @Test
    public void testDetectsDuplicatePropertyDescriptorNames() {
        init(new DuplicateNameInPropertyDescription());
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> processorSetupValidator.validatePropertyDescriptors());
        assertTrue(error.getMessage().contains("Detected duplicate property names"));
    }

    @Test
    public void testDetectMissingDeclaredProperties() {
        init(new MissingFromPropertyDescriptorSet());
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> processorSetupValidator.validatePropertyDescriptors());
        assertTrue(error.getMessage().contains("was not in the list returned by declaredDescriptors"));
    }

    @Test
    public void testMissingServicesFolder() {
        processorSetupValidator = new EmptyClasspathProcessorSetupValidator();
        init(new MightBeMissingFromProcessorConfigFile());
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> processorSetupValidator.validateExistenceOfServicesConf());
        assertTrue(error.getMessage().contains("No META-INF/services folder found on the classpath"));
    }

    @Test
    public void testConstructorWithAllValidation() {
        init(new MightBeMissingFromProcessorConfigFile());
        assertDoesNotThrow(() -> new ProcessorSetupValidator(processor));
    }

    @Test
    public void testPresentServicesFolder() {
        init(new MightBeMissingFromProcessorConfigFile());
        assertDoesNotThrow(() -> processorSetupValidator.validateExistenceOfServicesConf());
    }

    @Test
    public void testCheckExistenceInServicesConfig() {
        init(new MightBeMissingFromProcessorConfigFile());
        assertDoesNotThrow(() -> processorSetupValidator.validateExistanceOfComponentInServices());
    }

    @Test
    public void testMissingFromServiceConfigThrowsException() {
        processorSetupValidator = new WrongClasspathProcessorSetupValidator();
        init(new MightBeMissingFromProcessorConfigFile());
        AssertionFailedError error = assertThrows(AssertionFailedError.class,
                () -> processorSetupValidator.validateExistanceOfComponentInServices());
        assertTrue(error.getMessage().contains("was not in"));
    }

    @Test
    public void testMissingRelationshipsShouldThrowException() {
        init(new MightBeMissingFromProcessorConfigFile(false));
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> processorSetupValidator.validateRelationships());
        assertTrue(error.getMessage().contains("not in relationship set"));
    }
}

@CapabilityDescription("Hello!")
class OnlyMissingTags extends NoOpProcessor {
}

@Tags({"x", "y"})
class OnlyMissingCapabilitiyDescription extends NoOpProcessor {
}

@Tags({"x", "y"})
@CapabilityDescription("Hello!")
class DuplicateNameInPropertyDescription extends NoOpProcessor {
    public static final PropertyDescriptor TEST = new PropertyDescriptor.Builder()
            .name("test-test")
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor TEST2 = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(TEST)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(TEST, TEST2);
    }
}

@Tags({"x", "y"})
@CapabilityDescription("Hello!")
class MissingFromPropertyDescriptorSet extends NoOpProcessor {
    public static final PropertyDescriptor TEST = new PropertyDescriptor.Builder()
            .name("test-test")
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor TEST2 = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(TEST)
            .name("test-test2")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(TEST);
    }
}

@Tags({"x", "y"})
@CapabilityDescription("Hello!")
class MightBeMissingFromProcessorConfigFile extends NoOpProcessor {
    public static final PropertyDescriptor TEST = new PropertyDescriptor.Builder()
            .name("test-test")
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor TEST2 = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(TEST)
            .name("test-test2")
            .build();

    private Set<Relationship> relationshipSet;

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").build();

    public MightBeMissingFromProcessorConfigFile() {
        this(true);
    }

    public MightBeMissingFromProcessorConfigFile(boolean allRelationships) {
        if (allRelationships) {
            relationshipSet = new HashSet<>(Arrays.asList(
                    REL_SUCCESS, REL_FAILURE
            ));
        } else {
            relationshipSet = new HashSet<>(Arrays.asList(
                    REL_SUCCESS
            ));
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationshipSet;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(TEST, TEST2);
    }
}

class EmptyClasspathProcessorSetupValidator extends ProcessorSetupValidator {
    @Override
    protected InputStream getClasspathResourceAsInputStream(String path) {
        return null;
    }

    @Override
    protected URL getClasspathResource(String path) {
        return null;
    }
}

class WrongClasspathProcessorSetupValidator extends ProcessorSetupValidator {
    @Override
    protected InputStream getClasspathResourceAsInputStream(String path) {
        return new ByteArrayInputStream(new StringBuilder()
                .append("x").append("\n\n\n").append("y").toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected URL getClasspathResource(String path) {
        try {
            return new URL("https://google.com");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}