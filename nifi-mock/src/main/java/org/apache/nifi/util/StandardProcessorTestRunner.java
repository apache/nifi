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
package org.apache.nifi.util;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.junit.Assert;

public class StandardProcessorTestRunner implements TestRunner {

    private final Processor processor;
    private final MockProcessContext context;
    private final MockFlowFileQueue flowFileQueue;
    private final SharedSessionState sharedState;
    private final AtomicLong idGenerator;
    private final boolean triggerSerially;
    private final MockStateManager processorStateManager;
    private final Map<String, MockStateManager> controllerServiceStateManagers = new HashMap<>();
    private final MockVariableRegistry variableRegistry;

    private int numThreads = 1;
    private MockSessionFactory sessionFactory;
    private long runSchedule = 0;
    private final AtomicInteger invocations = new AtomicInteger(0);

    private final Map<String, MockComponentLog> controllerServiceLoggers = new HashMap<>();
    private final MockComponentLog logger;
    private boolean enforceReadStreamsClosed = true;

    StandardProcessorTestRunner(final Processor processor) {
        this.processor = processor;
        this.idGenerator = new AtomicLong(0L);
        this.sharedState = new SharedSessionState(processor, idGenerator);
        this.flowFileQueue = sharedState.getFlowFileQueue();
        this.sessionFactory = new MockSessionFactory(sharedState, processor, enforceReadStreamsClosed);
        this.processorStateManager = new MockStateManager(processor);
        this.variableRegistry = new MockVariableRegistry();
        this.context = new MockProcessContext(processor, processorStateManager, variableRegistry);

        final MockProcessorInitializationContext mockInitContext = new MockProcessorInitializationContext(processor, context);
        processor.initialize(mockInitContext);
        logger =  mockInitContext.getLogger();

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, processor);
        } catch (final Exception e) {
            Assert.fail("Could not invoke methods annotated with @OnAdded annotation due to: " + e);
        }

        triggerSerially = null != processor.getClass().getAnnotation(TriggerSerially.class);

        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, processor);
    }

    @Override
    public void enforceReadStreamsClosed(final boolean enforce) {
        enforceReadStreamsClosed = enforce;
        this.sessionFactory = new MockSessionFactory(sharedState, processor, enforceReadStreamsClosed);
    }

    @Override
    public void setValidateExpressionUsage(final boolean validate) {
        context.setValidateExpressionUsage(validate);
    }

    @Override
    public Processor getProcessor() {
        return processor;
    }

    @Override
    public MockProcessContext getProcessContext() {
        return context;
    }

    @Override
    public void run() {
        run(1);
    }

    @Override
    public void run(int iterations) {
        run(iterations, true);
    }

    @Override
    public void run(final int iterations, final boolean stopOnFinish) {
        run(iterations, stopOnFinish, true);
    }

    @Override
    public void run(final int iterations, final boolean stopOnFinish, final boolean initialize) {
        run(iterations, stopOnFinish, initialize, 5000);
    }

    @Override
    public void run(final int iterations, final boolean stopOnFinish, final boolean initialize, final long runWait) {
        if (iterations < 1) {
            throw new IllegalArgumentException();
        }

        context.assertValid();
        context.enableExpressionValidation();
        try {
            if (initialize) {
                try {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, processor, context);
                } catch (final Exception e) {
                    e.printStackTrace();
                    Assert.fail("Could not invoke methods annotated with @OnScheduled annotation due to: " + e);
                }
            }

            final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(numThreads);
            @SuppressWarnings("unchecked")
            final Future<Throwable>[] futures = new Future[iterations];
            for (int i = 0; i < iterations; i++) {
                final Future<Throwable> future = executorService.schedule(new RunProcessor(), i * runSchedule, TimeUnit.MILLISECONDS);
                futures[i] = future;
            }

            executorService.shutdown();
            try {
                executorService.awaitTermination(runWait, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e1) {
            }

            int finishedCount = 0;
            boolean unscheduledRun = false;
            for (final Future<Throwable> future : futures) {
                try {
                    final Throwable thrown = future.get(); // wait for the result
                    if (thrown != null) {
                        throw new AssertionError(thrown);
                    }

                    if (++finishedCount == 1) {
                        unscheduledRun = true;
                        try {
                            ReflectionUtils.invokeMethodsWithAnnotation(OnUnscheduled.class, processor, context);
                        } catch (final Exception e) {
                            Assert.fail("Could not invoke methods annotated with @OnUnscheduled annotation due to: " + e);
                        }
                    }
                } catch (final Exception e) {
                }
            }

            if (!unscheduledRun) {
                try {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnUnscheduled.class, processor, context);
                } catch (final Exception e) {
                    Assert.fail("Could not invoke methods annotated with @OnUnscheduled annotation due to: " + e);
                }
            }

            if (stopOnFinish) {
                try {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, processor, context);
                } catch (final Exception e) {
                    Assert.fail("Could not invoke methods annotated with @OnStopped annotation due to: " + e);
                }
            }
        } finally {
            context.disableExpressionValidation();
        }
    }

    @Override
    public void shutdown() {
        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnShutdown.class, processor);
        } catch (final Exception e) {
            Assert.fail("Could not invoke methods annotated with @OnShutdown annotation due to: " + e);
        }
    }

    private class RunProcessor implements Callable<Throwable> {

        @Override
        public Throwable call() throws Exception {
            invocations.incrementAndGet();
            try {
                processor.onTrigger(context, sessionFactory);
            } catch (final Throwable t) {
                return t;
            }

            return null;
        }
    }

    @Override
    public ProcessSessionFactory getProcessSessionFactory() {
        return sessionFactory;
    }

    @Override
    public void assertAllFlowFilesTransferred(final String relationship) {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.assertAllFlowFilesTransferred(relationship);
        }
    }

    @Override
    public void assertAllFlowFilesTransferred(final Relationship relationship) {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.assertAllFlowFilesTransferred(relationship);
        }
    }

    @Override
    public void assertAllFlowFilesTransferred(final String relationship, final int count) {
        assertAllFlowFilesTransferred(relationship);
        assertTransferCount(relationship, count);
    }

    @Override
    public void assertAllFlowFilesContainAttribute(final String attributeName) {
        assertAllFlowFiles(new FlowFileValidator() {
            @Override
            public void assertFlowFile(FlowFile f) {
                Assert.assertTrue(f.getAttribute(attributeName) != null);
            }
        });
    }

    @Override
    public void assertAllFlowFilesContainAttribute(final Relationship relationship, final String attributeName) {
        assertAllFlowFiles(relationship, new FlowFileValidator() {
            @Override
            public void assertFlowFile(FlowFile f) {
                Assert.assertTrue(f.getAttribute(attributeName) != null);
            }
        });
    }

    @Override
    public void assertAllFlowFiles(FlowFileValidator validator) {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.assertAllFlowFiles(validator);
        }
    }

    @Override
    public void assertAllFlowFiles(Relationship relationship, FlowFileValidator validator) {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.assertAllFlowFiles(relationship, validator);
        }
    }

    @Override
    public void assertAllFlowFilesTransferred(final Relationship relationship, final int count) {
        assertAllFlowFilesTransferred(relationship);
        assertTransferCount(relationship, count);
    }

    @Override
    public void assertTransferCount(final Relationship relationship, final int count) {
        Assert.assertEquals(count, getFlowFilesForRelationship(relationship).size());
    }

    @Override
    public void assertTransferCount(final String relationship, final int count) {
        Assert.assertEquals(count, getFlowFilesForRelationship(relationship).size());
    }

    @Override
    public void assertPenalizeCount(final int count) {
        Assert.assertEquals(count, getPenalizedFlowFiles().size());
    }

    @Override
    public void assertValid() {
        context.assertValid();
    }

    @Override
    public void assertNotValid() {
        Assert.assertFalse("Processor appears to be valid but expected it to be invalid", context.isValid());
    }

    @Override
    public boolean isQueueEmpty() {
        return flowFileQueue.isEmpty();
    }

    @Override
    public void assertQueueEmpty() {
        Assert.assertTrue(flowFileQueue.isEmpty());
    }

    @Override
    public void assertQueueNotEmpty() {
        Assert.assertFalse(flowFileQueue.isEmpty());
    }

    @Override
    public void clearTransferState() {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.clearTransferState();
        }
    }

    @Override
    public void enqueue(final FlowFile... flowFiles) {
        for (final FlowFile flowFile : flowFiles) {
            flowFileQueue.offer((MockFlowFile) flowFile);
        }
    }

    @Override
    public MockFlowFile enqueue(final Path path) throws IOException {
        return enqueue(path, new HashMap<String, String>());
    }

    @Override
    public MockFlowFile enqueue(final Path path, final Map<String, String> attributes) throws IOException {
        final Map<String, String> modifiedAttributes = new HashMap<>(attributes);
        if (!modifiedAttributes.containsKey(CoreAttributes.FILENAME.key())) {
            modifiedAttributes.put(CoreAttributes.FILENAME.key(), path.toFile().getName());
        }
        try (final InputStream in = Files.newInputStream(path)) {
            return enqueue(in, modifiedAttributes);
        }
    }

    @Override
    public MockFlowFile enqueue(final byte[] data) {
        return enqueue(data, new HashMap<String, String>());
    }

    @Override
    public MockFlowFile enqueue(final String data) {
        return enqueue(data.getBytes(StandardCharsets.UTF_8), Collections.<String, String> emptyMap());
    }

    @Override
    public MockFlowFile enqueue(final byte[] data, final Map<String, String> attributes) {
        return enqueue(new ByteArrayInputStream(data), attributes);
    }

    @Override
    public MockFlowFile enqueue(final String data, final Map<String, String> attributes) {
        return enqueue(data.getBytes(StandardCharsets.UTF_8), attributes);
    }


    @Override
    public MockFlowFile enqueue(final InputStream data) {
        return enqueue(data, new HashMap<String, String>());
    }

    @Override
    public MockFlowFile enqueue(final InputStream data, final Map<String, String> attributes) {
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, idGenerator), processor, enforceReadStreamsClosed);
        MockFlowFile flowFile = session.create();
        flowFile = session.importFrom(data, flowFile);
        flowFile = session.putAllAttributes(flowFile, attributes);
        enqueue(flowFile);
        return flowFile;
    }

    @Override
    public byte[] getContentAsByteArray(final MockFlowFile flowFile) {
        return flowFile.getData();
    }

    @Override
    public List<MockFlowFile> getFlowFilesForRelationship(final String relationship) {
        final Relationship rel = new Relationship.Builder().name(relationship).build();
        return getFlowFilesForRelationship(rel);
    }

    @Override
    public List<MockFlowFile> getFlowFilesForRelationship(final Relationship relationship) {
        final List<MockFlowFile> flowFiles = new ArrayList<>();
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            flowFiles.addAll(session.getFlowFilesForRelationship(relationship));
        }

        Collections.sort(flowFiles, new Comparator<MockFlowFile>() {
            @Override
            public int compare(final MockFlowFile o1, final MockFlowFile o2) {
                return Long.compare(o1.getCreationTime(), o2.getCreationTime());
            }
        });

        return flowFiles;
    }

    @Override
    public List<MockFlowFile> getPenalizedFlowFiles() {
        final List<MockFlowFile> flowFiles = new ArrayList<>();
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            flowFiles.addAll(session.getPenalizedFlowFiles());
        }

        Collections.sort(flowFiles, new Comparator<MockFlowFile>() {
            @Override
            public int compare(final MockFlowFile o1, final MockFlowFile o2) {
                return Long.compare(o1.getCreationTime(), o2.getCreationTime());
            }
        });

        return flowFiles;
    }

    @Override
    public QueueSize getQueueSize() {
        return flowFileQueue.size();
    }

    public void clearQueue() {
        // TODO: Add #clear to MockFlowFileQueue or just point to new instance?
        while (!flowFileQueue.isEmpty()) {
            flowFileQueue.poll();
        }
    }

    @Override
    public Long getCounterValue(final String name) {
        return sharedState.getCounterValue(name);
    }

    @Override
    public int getRemovedCount() {
        int count = 0;
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            count += session.getRemovedCount();
        }

        return count;
    }

    @Override
    public void setAnnotationData(final String annotationData) {
        context.setAnnotationData(annotationData);
    }

    @Override
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return context.setProperty(propertyName, propertyValue);
    }

    @Override
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final String value) {
        return context.setProperty(descriptor, value);
    }

    @Override
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final AllowableValue value) {
        return context.setProperty(descriptor, value.getValue());
    }

    @Override
    public void setThreadCount(final int threadCount) {
        if (threadCount > 1 && triggerSerially) {
            Assert.fail("Cannot set thread-count higher than 1 because the processor is triggered serially");
        }

        this.numThreads = threadCount;
        this.context.setMaxConcurrentTasks(threadCount);
    }

    @Override
    public int getThreadCount() {
        return numThreads;
    }

    @Override
    public void setRelationshipAvailable(final Relationship relationship) {
        final Set<Relationship> unavailable = new HashSet<>(context.getUnavailableRelationships());
        unavailable.remove(relationship);
        context.setUnavailableRelationships(unavailable);
    }

    @Override
    public void setRelationshipAvailable(final String relationshipName) {
        setRelationshipAvailable(new Relationship.Builder().name(relationshipName).build());
    }

    @Override
    public void setRelationshipUnavailable(final Relationship relationship) {
        final Set<Relationship> unavailable = new HashSet<>(context.getUnavailableRelationships());
        unavailable.add(relationship);
        context.setUnavailableRelationships(unavailable);
    }

    @Override
    public void setRelationshipUnavailable(final String relationshipName) {
        setRelationshipUnavailable(new Relationship.Builder().name(relationshipName).build());
    }

    @Override
    public void setIncomingConnection(boolean hasIncomingConnection) {
        context.setIncomingConnection(hasIncomingConnection);
    }

    @Override
    public void setNonLoopConnection(final boolean hasNonLoopConnection) {
        context.setNonLoopConnection(hasNonLoopConnection);
    }

    @Override
    public void addConnection(Relationship relationship) {
        context.addConnection(relationship);
    }

    @Override
    public void addConnection(String relationshipName) {
        addConnection(new Relationship.Builder().name(relationshipName).build());
    }

    @Override
    public void removeConnection(Relationship relationship) {
        context.removeConnection(relationship);
    }

    @Override
    public void removeConnection(String relationshipName) {
        removeConnection(new Relationship.Builder().name(relationshipName).build());
    }

    @Override
    public void addControllerService(final String identifier, final ControllerService service) throws InitializationException {
        addControllerService(identifier, service, new HashMap<String, String>());
    }

    @Override
    public void addControllerService(final String identifier, final ControllerService service, final Map<String, String> properties) throws InitializationException {
        final MockComponentLog logger = new MockComponentLog(identifier, service);
        controllerServiceLoggers.put(identifier, logger);
        final MockStateManager serviceStateManager = new MockStateManager(service);
        final MockControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(requireNonNull(service), requireNonNull(identifier), logger, serviceStateManager);
        controllerServiceStateManagers.put(identifier, serviceStateManager);
        initContext.addControllerServices(context);
        service.initialize(initContext);

        final Map<PropertyDescriptor, String> resolvedProps = new HashMap<>();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            resolvedProps.put(service.getPropertyDescriptor(entry.getKey()), entry.getValue());
        }

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, service);
        } catch (final InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
            throw new InitializationException(e);
        }

        context.addControllerService(identifier, service, resolvedProps, null);
    }

    @Override
    public void assertNotValid(final ControllerService service) {
        final StateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller Service has not been added to this TestRunner via the #addControllerService method");
        }

        final ValidationContext validationContext = new MockValidationContext(context, serviceStateManager, variableRegistry).getControllerServiceValidationContext(service);
        final Collection<ValidationResult> results = context.getControllerService(service.getIdentifier()).validate(validationContext);

        for (final ValidationResult result : results) {
            if (!result.isValid()) {
                return;
            }
        }

        Assert.fail("Expected Controller Service " + service + " to be invalid but it is valid");
    }

    @Override
    public void assertValid(final ControllerService service) {
        final StateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller Service has not been added to this TestRunner via the #addControllerService method");
        }

        final ValidationContext validationContext = new MockValidationContext(context, serviceStateManager, variableRegistry).getControllerServiceValidationContext(service);
        final Collection<ValidationResult> results = context.getControllerService(service.getIdentifier()).validate(validationContext);

        for (final ValidationResult result : results) {
            if (!result.isValid()) {
                Assert.fail("Expected Controller Service to be valid but it is invalid due to: " + result.toString());
            }
        }
    }

    @Override
    public void disableControllerService(final ControllerService service) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (!configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be disabled because it is not enabled");
        }

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnDisabled.class, service);
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to disable Controller Service " + service + " due to " + e);
        }

        configuration.setEnabled(false);
    }

    @Override
    public void enableControllerService(final ControllerService service) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Cannot enable Controller Service " + service + " because it is not disabled");
        }

        try {
            final ConfigurationContext configContext = new MockConfigurationContext(service, configuration.getProperties(), context,variableRegistry);
            ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, service, configContext);
        } catch (final InvocationTargetException ite) {
            ite.getCause().printStackTrace();
            Assert.fail("Failed to enable Controller Service " + service + " due to " + ite.getCause());
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to enable Controller Service " + service + " due to " + e);
        }

        configuration.setEnabled(true);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        return configuration.isEnabled();
    }

    @Override
    public void removeControllerService(final ControllerService service) {
        disableControllerService(service);

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnRemoved.class, service);
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to remove Controller Service " + service + " due to " + e);
        }

        context.removeControllerService(service);
    }

    @Override
    public void setAnnotationData(final ControllerService service, final String annotationData) {
        final ControllerServiceConfiguration configuration = getConfigToUpdate(service);
        configuration.setAnnotationData(annotationData);
    }

    private ControllerServiceConfiguration getConfigToUpdate(final ControllerService service) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be modified because it is not disabled");
        }

        return configuration;
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final PropertyDescriptor property, final AllowableValue value) {
        return setProperty(service, property, value.getValue());
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final PropertyDescriptor property, final String value) {
        final MockStateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller service " + service + " has not been added to this TestRunner via the #addControllerService method");
        }

        final ControllerServiceConfiguration configuration = getConfigToUpdate(service);
        final Map<PropertyDescriptor, String> curProps = configuration.getProperties();
        final Map<PropertyDescriptor, String> updatedProps = new HashMap<>(curProps);

        final ValidationContext validationContext = new MockValidationContext(context, serviceStateManager, variableRegistry).getControllerServiceValidationContext(service);
        final ValidationResult validationResult = property.validate(value, validationContext);

        final String oldValue = updatedProps.get(property);
        updatedProps.put(property, value);
        configuration.setProperties(updatedProps);

        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            service.onPropertyModified(property, oldValue, value);
        }

        return validationResult;
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final String propertyName, final String value) {
        final PropertyDescriptor descriptor = service.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return new ValidationResult.Builder()
                .input(propertyName)
                .explanation(propertyName + " is not a known Property for Controller Service " + service)
                .subject("Invalid property")
                .valid(false)
                .build();
        }
        return setProperty(service, descriptor, value);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        return context.getControllerService(identifier);
    }

    @Override
    public <T extends ControllerService> T getControllerService(final String identifier, final Class<T> serviceType) {
        final ControllerService service = context.getControllerService(identifier);
        return serviceType.cast(service);
    }

    @Override
    public boolean removeProperty(PropertyDescriptor descriptor) {
        return context.removeProperty(descriptor);
    }

    @Override
    public List<ProvenanceEventRecord> getProvenanceEvents() {
        return sharedState.getProvenanceEvents();
    }

    @Override
    public void clearProvenanceEvents() {
        sharedState.clearProvenanceEvents();
    }

    @Override
    public MockStateManager getStateManager() {
        return processorStateManager;
    }

    /**
     * Returns the State Manager for the given Controller Service.
     *
     * @param controllerService the Controller Service whose State Manager should be returned
     * @return the State Manager for the given Controller Service
     */
    @Override
    public MockStateManager getStateManager(final ControllerService controllerService) {
        return controllerServiceStateManagers.get(controllerService.getIdentifier());
    }

    @Override
    public MockComponentLog getLogger() {
        return logger;
    }

    @Override
    public MockComponentLog getControllerServiceLogger(final String identifier) {
        return controllerServiceLoggers.get(identifier);
    }

    @Override
    public void setClustered(boolean clustered) {
        context.setClustered(clustered);
    }

    @Override
    public void setPrimaryNode(boolean primaryNode) {
        context.setPrimaryNode(primaryNode);
    }

    @Override
    public String getVariableValue(final String name) {
        Objects.requireNonNull(name);

        return variableRegistry.getVariableValue(name);
    }

    @Override
    public void setVariable(final String name, final String value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);

        final VariableDescriptor descriptor = new VariableDescriptor.Builder(name).build();
        variableRegistry.setVariable(descriptor, value);
    }

    @Override
    public String removeVariable(final String name) {
        Objects.requireNonNull(name);

        return variableRegistry.removeVariable(new VariableDescriptor.Builder(name).build());
    }

    /**
     * Asserts that all FlowFiles meet all conditions.
     *
     * @param relationshipName relationship name
     * @param predicate conditions
     */
    @Override
    public void assertAllConditionsMet(final String relationshipName, Predicate<MockFlowFile> predicate) {
        assertAllConditionsMet(new Relationship.Builder().name(relationshipName).build(), predicate);
    }

    /**
     * Asserts that all FlowFiles meet all conditions.
     *
     * @param relationship relationship
     * @param predicate conditions
     */
    @Override
    public void assertAllConditionsMet(final Relationship relationship, Predicate<MockFlowFile> predicate) {

        if (predicate==null) {
            Assert.fail("predicate cannot be null");
        }

        final List<MockFlowFile> flowFiles = getFlowFilesForRelationship(relationship);

        if (flowFiles.isEmpty()) {
            Assert.fail("Relationship " + relationship.getName() + " does not contain any FlowFile");
        }

        for (MockFlowFile flowFile : flowFiles) {
            if (predicate.test(flowFile)==false) {
                Assert.fail("FlowFile " + flowFile + " does not meet all condition");
            }
        }
    }

    /**
     * Set the Run Schedule parameter (in milliseconds). If set, this will be the duration
     * between two calls of the onTrigger method.
     *
     * @param runSchedule Run schedule duration in milliseconds.
     */
    @Override
    public void setRunSchedule(long runSchedule) {
        this.runSchedule = runSchedule;
    }
}
