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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.test.processors.ModifiesClasspathNoAnnotationProcessor;
import org.apache.nifi.test.processors.ModifiesClasspathProcessor;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.MockVariableRegistry;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStandardProcessorNode {

    private MockVariableRegistry variableRegistry;

    @Before
    public void setup() {
        variableRegistry = new MockVariableRegistry();
    }

    @Test(timeout = 10000)
    public void testStart() throws InterruptedException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestStandardProcessorNode.class.getResource("/conf/nifi.properties").getFile());
        final ProcessorThatThrowsExceptionOnScheduled processor = new ProcessorThatThrowsExceptionOnScheduled();
        final String uuid = UUID.randomUUID().toString();

        ProcessorInitializationContext initContext = new StandardProcessorInitializationContext(uuid, null, null, null, null);
        processor.initialize(initContext);

        final StandardProcessorNode procNode = new StandardProcessorNode(processor, uuid, createValidationContextFactory(), null, null,
                NiFiProperties.createBasicNiFiProperties(null, null), VariableRegistry.EMPTY_REGISTRY, Mockito.mock(ComponentLog.class));
        final ScheduledExecutorService taskScheduler = new FlowEngine(2, "TestClasspathResources", true);

        final StandardProcessContext processContext = new StandardProcessContext(procNode, null, null, null, null);
        final SchedulingAgentCallback schedulingAgentCallback = new SchedulingAgentCallback() {
            @Override
            public void postMonitor() {
            }

            @Override
            public Future<?> invokeMonitoringTask(final Callable<?> task) {
                return taskScheduler.submit(task);
            }

            @Override
            public void trigger() {
                Assert.fail("Should not have completed");
            }
        };

        procNode.start(taskScheduler, 20000L, processContext, schedulingAgentCallback);

        Thread.sleep(1000L);
        assertEquals(1, processor.onScheduledCount);
        assertEquals(1, processor.onUnscheduledCount);
        assertEquals(1, processor.onStoppedCount);
    }

    @Test
    public void testSinglePropertyDynamicallyModifiesClasspath() throws MalformedURLException {
        final PropertyDescriptor classpathProp = new PropertyDescriptor.Builder().name("Classpath Resources")
                .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp));
        final StandardProcessorNode procNode = createProcessorNode(processor);

        final Set<ClassLoader> classLoaders = new HashSet<>();
        classLoaders.add(procNode.getProcessor().getClass().getClassLoader());

        // Load all of the extensions in src/test/java of this project
        ExtensionManager.discoverExtensions(classLoaders);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getIdentifier())){
            // Should have an InstanceClassLoader here
            final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            assertTrue(contextClassLoader instanceof InstanceClassLoader);

            final InstanceClassLoader instanceClassLoader = (InstanceClassLoader) contextClassLoader;

            // Should not have any of the test resources loaded at this point
            final URL[] testResources = getTestResources();
            for (URL testResource : testResources) {
                if (containsResource(instanceClassLoader.getInstanceResources(), testResource)) {
                    fail("found resource that should not have been loaded");
                }
            }

            // Simulate setting the properties of the processor to point to the test resources directory
            final Map<String, String> properties = new HashMap<>();
            properties.put(classpathProp.getName(), "src/test/resources/TestClasspathResources");
            procNode.setProperties(properties);

            // Should have all of the resources loaded into the InstanceClassLoader now
            for (URL testResource : testResources) {
                assertTrue(containsResource(instanceClassLoader.getInstanceResources(), testResource));
            }

            // Should pass validation
            assertTrue(procNode.isValid());
        } finally {
            ExtensionManager.removeInstanceClassLoaderIfExists(procNode.getIdentifier());
        }
    }

    @Test
    public void testUpdateOtherPropertyDoesNotImpactClasspath() throws MalformedURLException {
        final PropertyDescriptor classpathProp = new PropertyDescriptor.Builder().name("Classpath Resources")
                .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final PropertyDescriptor otherProp = new PropertyDescriptor.Builder().name("My Property")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp, otherProp));
        final StandardProcessorNode procNode = createProcessorNode(processor);

        final Set<ClassLoader> classLoaders = new HashSet<>();
        classLoaders.add(procNode.getProcessor().getClass().getClassLoader());

        // Load all of the extensions in src/test/java of this project
        ExtensionManager.discoverExtensions(classLoaders);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getIdentifier())){
            // Should have an InstanceClassLoader here
            final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            assertTrue(contextClassLoader instanceof InstanceClassLoader);

            final InstanceClassLoader instanceClassLoader = (InstanceClassLoader) contextClassLoader;

            // Should not have any of the test resources loaded at this point
            final URL[] testResources = getTestResources();
            for (URL testResource : testResources) {
                if (containsResource(instanceClassLoader.getInstanceResources(), testResource)) {
                    fail("found resource that should not have been loaded");
                }
            }

            // Simulate setting the properties of the processor to point to the test resources directory
            final Map<String, String> properties = new HashMap<>();
            properties.put(classpathProp.getName(), "src/test/resources/TestClasspathResources");
            procNode.setProperties(properties);

            // Should have all of the resources loaded into the InstanceClassLoader now
            for (URL testResource : testResources) {
                assertTrue(containsResource(instanceClassLoader.getInstanceResources(), testResource));
            }

            // Should pass validation
            assertTrue(procNode.isValid());

            // Simulate setting updating the other property which should not change the classpath
            final Map<String, String> otherProperties = new HashMap<>();
            otherProperties.put(otherProp.getName(), "foo");
            procNode.setProperties(otherProperties);

            // Should STILL have all of the resources loaded into the InstanceClassLoader now
            for (URL testResource : testResources) {
                assertTrue(containsResource(instanceClassLoader.getInstanceResources(), testResource));
            }

            // Should STILL pass validation
            assertTrue(procNode.isValid());

            // Lets update the classpath property and make sure the resources get updated
            final Map<String, String> newClasspathProperties = new HashMap<>();
            newClasspathProperties.put(classpathProp.getName(), "src/test/resources/TestClasspathResources/resource1.txt");
            procNode.setProperties(newClasspathProperties);

            // Should only have resource1 loaded now
            assertTrue(containsResource(instanceClassLoader.getInstanceResources(), testResources[0]));
            assertFalse(containsResource(instanceClassLoader.getInstanceResources(), testResources[1]));
            assertFalse(containsResource(instanceClassLoader.getInstanceResources(), testResources[2]));

            // Should STILL pass validation
            assertTrue(procNode.isValid());
        } finally {
            ExtensionManager.removeInstanceClassLoaderIfExists(procNode.getIdentifier());
        }
    }

    @Test
    public void testMultiplePropertiesDynamicallyModifyClasspathWithExpressionLanguage() throws MalformedURLException {
        final PropertyDescriptor classpathProp1 = new PropertyDescriptor.Builder().name("Classpath Resource 1")
                .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        final PropertyDescriptor classpathProp2 = new PropertyDescriptor.Builder().name("Classpath Resource 2")
                .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp1, classpathProp2));
        final StandardProcessorNode procNode = createProcessorNode(processor);

        final Set<ClassLoader> classLoaders = new HashSet<>();
        classLoaders.add(procNode.getProcessor().getClass().getClassLoader());

        // Load all of the extensions in src/test/java of this project
        ExtensionManager.discoverExtensions(classLoaders);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getIdentifier())){
            // Should have an InstanceClassLoader here
            final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            assertTrue(contextClassLoader instanceof InstanceClassLoader);

            final InstanceClassLoader instanceClassLoader = (InstanceClassLoader) contextClassLoader;

            // Should not have any of the test resources loaded at this point
            final URL[] testResources = getTestResources();
            for (URL testResource : testResources) {
                if (containsResource(instanceClassLoader.getInstanceResources(), testResource)) {
                    fail("found resource that should not have been loaded");
                }
            }

            // Simulate setting the properties pointing to two of the resources
            final Map<String, String> properties = new HashMap<>();
            properties.put(classpathProp1.getName(), "src/test/resources/TestClasspathResources/resource1.txt");
            properties.put(classpathProp2.getName(), "src/test/resources/TestClasspathResources/${myResource}");

            variableRegistry.setVariable(new VariableDescriptor("myResource"), "resource3.txt");

            procNode.setProperties(properties);

            // Should have resources 1 and 3 loaded into the InstanceClassLoader now
            assertTrue(containsResource(instanceClassLoader.getInstanceResources(), testResources[0]));
            assertTrue(containsResource(instanceClassLoader.getInstanceResources(), testResources[2]));
            assertFalse(containsResource(instanceClassLoader.getInstanceResources(), testResources[1]));

            // Should pass validation
            assertTrue(procNode.isValid());
        } finally {
            ExtensionManager.removeInstanceClassLoaderIfExists(procNode.getIdentifier());
        }
    }

    @Test
    public void testSomeNonExistentPropertiesDynamicallyModifyClasspath() throws MalformedURLException {
        final PropertyDescriptor classpathProp1 = new PropertyDescriptor.Builder().name("Classpath Resource 1")
                .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        final PropertyDescriptor classpathProp2 = new PropertyDescriptor.Builder().name("Classpath Resource 2")
                .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp1, classpathProp2));
        final StandardProcessorNode procNode = createProcessorNode(processor);

        final Set<ClassLoader> classLoaders = new HashSet<>();
        classLoaders.add(procNode.getProcessor().getClass().getClassLoader());

        // Load all of the extensions in src/test/java of this project
        ExtensionManager.discoverExtensions(classLoaders);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getIdentifier())){
            // Should have an InstanceClassLoader here
            final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            assertTrue(contextClassLoader instanceof InstanceClassLoader);

            final InstanceClassLoader instanceClassLoader = (InstanceClassLoader) contextClassLoader;

            // Should not have any of the test resources loaded at this point
            final URL[] testResources = getTestResources();
            for (URL testResource : testResources) {
                if (containsResource(instanceClassLoader.getInstanceResources(), testResource)) {
                    fail("found resource that should not have been loaded");
                }
            }

            // Simulate setting the properties pointing to two of the resources
            final Map<String, String> properties = new HashMap<>();
            properties.put(classpathProp1.getName(), "src/test/resources/TestClasspathResources/resource1.txt");
            properties.put(classpathProp2.getName(), "src/test/resources/TestClasspathResources/DoesNotExist.txt");
            procNode.setProperties(properties);

            // Should have resources 1 and 3 loaded into the InstanceClassLoader now
            assertTrue(containsResource(instanceClassLoader.getInstanceResources(), testResources[0]));
            assertFalse(containsResource(instanceClassLoader.getInstanceResources(), testResources[1]));
            assertFalse(containsResource(instanceClassLoader.getInstanceResources(), testResources[2]));

            // Should pass validation
            assertTrue(procNode.isValid());
        } finally {
            ExtensionManager.removeInstanceClassLoaderIfExists(procNode.getIdentifier());
        }
    }

    @Test
    public void testPropertyModifiesClasspathWhenProcessorMissingAnnotation() throws MalformedURLException {
        final ModifiesClasspathNoAnnotationProcessor processor = new ModifiesClasspathNoAnnotationProcessor();
        final StandardProcessorNode procNode = createProcessorNode(processor);

        final Set<ClassLoader> classLoaders = new HashSet<>();
        classLoaders.add(procNode.getProcessor().getClass().getClassLoader());

        // Load all of the extensions in src/test/java of this project
        ExtensionManager.discoverExtensions(classLoaders);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getIdentifier())){
            // Can't validate the ClassLoader here b/c the class is missing the annotation

            // Simulate setting the properties pointing to two of the resources
            final Map<String, String> properties = new HashMap<>();
            properties.put(ModifiesClasspathNoAnnotationProcessor.CLASSPATH_RESOURCE.getName(),
                    "src/test/resources/TestClasspathResources/resource1.txt");
            procNode.setProperties(properties);

            // Should not have loaded any of the resources
            final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            assertTrue(classLoader instanceof URLClassLoader);

            final URL[] testResources = getTestResources();
            final URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            assertFalse(containsResource(urlClassLoader.getURLs(), testResources[0]));
            assertFalse(containsResource(urlClassLoader.getURLs(), testResources[1]));
            assertFalse(containsResource(urlClassLoader.getURLs(), testResources[2]));

            // Should pass validation
            assertTrue(procNode.isValid());

        } finally {
            ExtensionManager.removeInstanceClassLoaderIfExists(procNode.getIdentifier());
        }
    }

    private StandardProcessorNode createProcessorNode(Processor processor) {
        final String uuid = UUID.randomUUID().toString();
        final ValidationContextFactory validationContextFactory = createValidationContextFactory();
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, null);
        final ProcessScheduler processScheduler = Mockito.mock(ProcessScheduler.class);
        final ComponentLog componentLog = Mockito.mock(ComponentLog.class);

        ProcessorInitializationContext initContext = new StandardProcessorInitializationContext(uuid, componentLog, null, null, null);
        processor.initialize(initContext);

        return new StandardProcessorNode(processor, uuid, validationContextFactory, processScheduler, null,
                niFiProperties, variableRegistry, componentLog);
    }

    private boolean containsResource(URL[] resources, URL resourceToFind) {
        for (URL resource : resources) {
            if (resourceToFind.getPath().equals(resource.getPath())) {
                return true;
            }
        }
        return false;
    }

    private URL[] getTestResources() throws MalformedURLException {
        URL resource1 = new File("src/test/resources/TestClasspathResources/resource1.txt").toURI().toURL();
        URL resource2 = new File("src/test/resources/TestClasspathResources/resource2.txt").toURI().toURL();
        URL resource3 = new File("src/test/resources/TestClasspathResources/resource3.txt").toURI().toURL();
        return new URL[] { resource1, resource2, resource3 };
    }


    private ValidationContextFactory createValidationContextFactory() {
        return new ValidationContextFactory() {
            @Override
            public ValidationContext newValidationContext(Map<PropertyDescriptor, String> properties, String annotationData, String groupId, String componentId) {
                return new ValidationContext() {

                    @Override
                    public ControllerServiceLookup getControllerServiceLookup() {
                        return null;
                    }

                    @Override
                    public ValidationContext getControllerServiceValidationContext(ControllerService controllerService) {
                        return null;
                    }

                    @Override
                    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
                        return null;
                    }

                    @Override
                    public PropertyValue getProperty(PropertyDescriptor property) {
                        return newPropertyValue(properties.get(property));
                    }

                    @Override
                    public PropertyValue newPropertyValue(String value) {
                        return new MockPropertyValue(value);
                    }

                    @Override
                    public Map<PropertyDescriptor, String> getProperties() {
                        return Collections.unmodifiableMap(properties);
                    }

                    @Override
                    public String getAnnotationData() {
                        return null;
                    }

                    @Override
                    public boolean isValidationRequired(ControllerService service) {
                        return false;
                    }

                    @Override
                    public boolean isExpressionLanguagePresent(String value) {
                        return false;
                    }

                    @Override
                    public boolean isExpressionLanguageSupported(String propertyName) {
                        return false;
                    }

                    @Override
                    public String getProcessGroupIdentifier() {
                        return groupId;
                    }
                };
            }

            @Override
            public ValidationContext newValidationContext(Set<String> serviceIdentifiersToNotValidate, Map<PropertyDescriptor, String> properties, String annotationData, String groupId,
                String componentId) {
                return newValidationContext(properties, annotationData, groupId, componentId);
            }
        };

    }


    public static class ProcessorThatThrowsExceptionOnScheduled extends AbstractProcessor {
        private int onScheduledCount = 0;
        private int onUnscheduledCount = 0;
        private int onStoppedCount = 0;

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }

        @OnScheduled
        public void onScheduled() {
            onScheduledCount++;
            throw new ProcessException("OnScheduled called - Unit Test throws Exception intentionally");
        }

        @OnUnscheduled
        public void onUnscheduled() {
            onUnscheduledCount++;
        }

        @OnStopped
        public void onStopped() {
            onStoppedCount++;
        }
    }

}
