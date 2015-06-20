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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.QueueSize;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.reporting.InitializationException;

public interface TestRunner {

    /**
     * @return the {@link Processor} for which this <code>TestRunner</code> is
     * configured
     */
    Processor getProcessor();

    /**
     * @return the {@link ProcessSessionFactory} that this
     * <code>TestRunner</code> will use to invoke the
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory)} method
     */
    ProcessSessionFactory getProcessSessionFactory();

    /**
     * @return the {@Link ProcessContext} that this <code>TestRunner</code> will
     * use to invoke the
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger}
     * method
     */
    ProcessContext getProcessContext();

    /**
     * Performs exactly the same operation as calling {@link #run(int)} with a
     * value of 1.
     */
    void run();

    /**
     * Performs the same operation as calling {@link #run(int, boolean)} with a
     * value of <code>true</code>
     *
     * @param iterations number of iterations
     */
    void run(int iterations);

    /**
     * performs the same operation as calling {@link #run(int, boolean, int)}
     * with a value of {@code iterations}, {@code stopOnFinish}, {@code true}
     *
     * @param iterations number of iterations
     * @param stopOnFinish flag to stop when finished
     */
    void run(int iterations, boolean stopOnFinish);

    /**
     * This method runs the {@link Processor} <code>iterations</code> times,
     * using the sequence of steps below:
     * <ul>
     * <li>
     * If {@code initialize} is true, run all methods on the Processor that are
     * annotated with the
     * {@link nifi.processor.annotation.OnScheduled @OnScheduled} annotation. If
     * any of these methods throws an Exception, the Unit Test will fail.
     * </li>
     * <li>
     * Schedule the
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger}
     * method to be invoked <code>iterations</code> times. The number of threads
     * used to run these iterations is determined by the ThreadCount of this
     * <code>TestRunner</code>. By default, the value is set to 1, but it can be
     * modified by calling the {@link #setThreadCount(int)} method.
     * </li>
     * <li>
     * As soon as the first thread finishes its execution of
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger},
     * all methods on the Processor that are annotated with the
     * {@link nifi.processor.annotation.OnUnscheduled @OnUnscheduled} annotation
     * are invoked. If any of these methods throws an Exception, the Unit Test
     * will fail.
     * </li>
     * <li>
     * Waits for all threads to finish execution.
     * </li>
     * <li>
     * If and only if the value of <code>shutdown</code> is true: Call all
     * methods on the Processor that is annotated with the
     * {@link nifi.processor.annotation.OnStopped @OnStopped} annotation.
     * </li>
     * </ul>
     *
     * @param iterations number of iterations
     * @param stopOnFinish whether or not to run the Processor methods that are
     * annotated with {@link nifi.processor.annotation.OnStopped @OnStopped}
     * @param initialize true if must initialize
     */
    void run(int iterations, boolean stopOnFinish, final boolean initialize);

    /**
     * This method runs the {@link Processor} <code>iterations</code> times,
     * using the sequence of steps below:
     * <ul>
     * <li>
     * If {@code initialize} is true, run all methods on the Processor that are
     * annotated with the
     * {@link nifi.processor.annotation.OnScheduled @OnScheduled} annotation. If
     * any of these methods throws an Exception, the Unit Test will fail.
     * </li>
     * <li>
     * Schedule the
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger}
     * method to be invoked <code>iterations</code> times. The number of threads
     * used to run these iterations is determined by the ThreadCount of this
     * <code>TestRunner</code>. By default, the value is set to 1, but it can be
     * modified by calling the {@link #setThreadCount(int)} method.
     * </li>
     * <li>
     * As soon as the first thread finishes its execution of
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger},
     * all methods on the Processor that are annotated with the
     * {@link nifi.processor.annotation.OnUnscheduled @OnUnscheduled} annotation
     * are invoked. If any of these methods throws an Exception, the Unit Test
     * will fail.
     * </li>
     * <li>
     * Waits for all threads to finish execution.
     * </li>
     * <li>
     * If and only if the value of <code>shutdown</code> is true: Call all
     * methods on the Processor that is annotated with the
     * {@link nifi.processor.annotation.OnStopped @OnStopped} annotation.
     * </li>
     * </ul>
     *
     * @param iterations number of iterations
     * @param stopOnFinish whether or not to run the Processor methods that are
     * annotated with {@link nifi.processor.annotation.OnStopped @OnStopped}
     * @param initialize true if must initialize
     * @param runWait indiciates the amount of time in milliseconds that the framework should wait for
     * processors to stop running before calling the {@link nifi.processor.annotation.OnUnscheduled @OnUnscheduled} annotation
     */
    void run(int iterations, boolean stopOnFinish, final boolean initialize, final long runWait);

    /**
     * Invokes all methods on the Processor that are annotated with the
     * {@link nifi.processor.annotation.OnShutdown @OnShutdown} annotation. If
     * any of these methods throws an Exception, the Unit Test will fail
     */
    void shutdown();

    /**
     * Updates the number of threads that will be used to run the Processor when
     * calling the {@link #run()} or {@link #run(int)} methods.
     *
     * @param threadCount num threads
     */
    void setThreadCount(int threadCount);

    /**
     * @return the currently configured number of threads that will be used to
     * runt he Processor when calling the {@link #run()} or {@link #run(int)}
     * methods
     */
    int getThreadCount();

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assert.fail() is called, causing the
     * unit test to fail
     *
     * @param propertyName name
     * @param propertyValue value
     * @return result
     */
    ValidationResult setProperty(String propertyName, String propertyValue);

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assert.fail() is called, causing the
     * unit test to fail
     *
     * @param descriptor descriptor
     * @param value value
     * @return result
     */
    ValidationResult setProperty(PropertyDescriptor descriptor, String value);

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assert.fail() is called, causing the
     * unit test to fail
     *
     * @param descriptor descriptor
     * @param value allowable valu
     * @return result
     */
    ValidationResult setProperty(PropertyDescriptor descriptor, AllowableValue value);

    /**
     * Sets the annotation data.
     *
     * @param annotationData data
     */
    void setAnnotationData(String annotationData);

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship
     *
     * @param relationship to verify
     */
    void assertAllFlowFilesTransferred(String relationship);

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship
     *
     * @param relationship to verify
     */
    void assertAllFlowFilesTransferred(Relationship relationship);

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship and that the number of FlowFiles transferred is equal
     * to <code>count</code>
     *
     * @param relationship to verify
     * @param count number of expected transfers
     */
    void assertAllFlowFilesTransferred(String relationship, int count);

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship and that the number of FlowFiles transferred is equal
     * to <code>count</code>
     *
     * @param relationship to verify
     * @param count number of expected transfers
     */
    void assertAllFlowFilesTransferred(Relationship relationship, int count);

    /**
     * Assert that the number of FlowFiles transferred to the given relationship
     * is equal to the given count
     *
     * @param relationship to verify
     * @param count number of expected transfers
     */
    void assertTransferCount(Relationship relationship, int count);

    /**
     * Assert that the number of FlowFiles transferred to the given relationship
     * is equal to the given count
     *
     * @param relationship to verify
     * @param count number of expected transfers
     */
    void assertTransferCount(String relationship, int count);

    /**
     * Assert that there are no FlowFiles left on the input queue.
     */
    void assertQueueEmpty();

    /**
     * @return <code>true</code> if the Input Queue to the Processor is empty,
     * <code>false</code> otherwise
     */
    boolean isQueueEmpty();

    /**
     * Assert that there is at least one FlowFile left on the input queue.
     */
    void assertQueueNotEmpty();

    /**
     * Assert that the currently configured set of properties/annotation data
     * are valid
     */
    void assertValid();

    /**
     * Assert that the currently configured set of properties/annotation data
     * are NOT valid
     */
    void assertNotValid();

    /**
     * Resets the Transfer Counts that indicate how many FlowFiles have been
     * transferred to each Relationship and removes from memory any FlowFiles
     * that have been transferred to this Relationships. This method should be
     * called between successive calls to {@link #run(int) run} if the output is
     * to be examined after each run.
     */
    void clearTransferState();

    /**
     * Enqueues the given FlowFiles into the Processor's input queue
     *
     * @param flowFiles to enqueue
     */
    void enqueue(FlowFile... flowFiles);

    /**
     * Reads the content from the given {@link Path} into memory and creates a
     * FlowFile from this content with no attributes and adds this FlowFile to
     * the Processor's Input Queue
     *
     * @param path to read content from
     * @throws IOException if unable to read content
     */
    void enqueue(Path path) throws IOException;

    /**
     * Reads the content from the given {@link Path} into memory and creates a
     * FlowFile from this content with the given attributes and adds this
     * FlowFile to the Processor's Input Queue
     *
     * @param path to read content from
     * @param attributes attributes to use for new flow file
     * @throws IOException if unable to read content
     */
    void enqueue(Path path, Map<String, String> attributes) throws IOException;

    /**
     * Copies the content from the given byte array into memory and creates a
     * FlowFile from this content with no attributes and adds this FlowFile to
     * the Processor's Input Queue
     *
     * @param data to enqueue
     */
    void enqueue(byte[] data);

    /**
     * Copies the content from the given byte array into memory and creates a
     * FlowFile from this content with the given attributes and adds this
     * FlowFile to the Processor's Input Queue
     *
     * @param data to enqueue
     * @param attributes to use for enqueued items
     */
    void enqueue(byte[] data, Map<String, String> attributes);

    /**
     * Reads the content from the given {@link InputStream} into memory and
     * creates a FlowFile from this content with no attributes and adds this
     * FlowFile to the Processor's Input Queue
     *
     * @param data to source data from
     */
    void enqueue(InputStream data);

    /**
     * Reads the content from the given {@link InputStream} into memory and
     * creates a FlowFile from this content with the given attributes and adds
     * this FlowFile to the Processor's Input Queue
     *
     * @param data source of data
     * @param attributes to use for flow files
     */
    void enqueue(InputStream data, Map<String, String> attributes);

    /**
     * Copies the contents of the given {@link MockFlowFile} into a byte array
     * and returns that byte array.
     *
     * @param flowFile to get content for
     * @return byte array of flowfile content
     */
    byte[] getContentAsByteArray(MockFlowFile flowFile);

    /**
     * Returns a List of FlowFiles in the order in which they were transferred
     * to the given relationship
     *
     * @param relationship to get flowfiles for
     * @return flowfiles transfered to given relationship
     */
    List<MockFlowFile> getFlowFilesForRelationship(String relationship);

    /**
     * Returns a List of FlowFiles in the order in which they were transferred
     * to the given relationship
     *
     * @param relationship to get flowfiles for
     * @return flowfiles transfered to given relationship
     */
    List<MockFlowFile> getFlowFilesForRelationship(Relationship relationship);

    /**
     * @return the {@link ProvenanceReporter} that will be used by the
     * configured {@link Processor} for reporting Provenance Events
     */
    ProvenanceReporter getProvenanceReporter();

    /**
     * @return the current size of the Processor's Input Queue
     */
    QueueSize getQueueSize();

    /**
     * @param name of counter
     * @return the current value of the counter with the specified name, or null
     * if no counter exists with the specified name
     */
    Long getCounterValue(String name);

    /**
     * @return the number of FlowFiles that have been removed from the system
     */
    int getRemovedCount();

    /**
     * Indicates to the Framework that the given Relationship should be
     * considered "available", meaning that the queues of all Connections that
     * contain this Relationship are not full. This is generally used only when
     * dealing with Processors that use the
     * {@link nifi.processor.annotation.TriggerWhenAnyDestinationAvailable}
     * annotation.
     *
     * @param relationship to mark as available
     */
    void setRelationshipAvailable(Relationship relationship);

    /**
     * Indicates to the Framework that the given Relationship with the given
     * name should be considered "available", meaning that the queues of all
     * Connections that contain this Relationship are not full. This is
     * generally used only when dealing with Processors that use the
     * {@link nifi.processor.annotation.TriggerWhenAnyDestinationAvailable}
     *
     * @param relationshipName relationship name
     */
    void setRelationshipAvailable(String relationshipName);

    /**
     * Indicates to the Framework that the given Relationship should NOT be
     * considered "available", meaning that the queue of at least one Connection
     * that contain this Relationship is full. This is generally used only when
     * dealing with Processors that use the
     * {@link nifi.processor.annotation.TriggerWhenAnyDestinationAvailable}
     * annotation.
     *
     * @param relationship to mark as unavailable
     */
    void setRelationshipUnavailable(Relationship relationship);

    /**
     * Indicates to the Framework that the Relationship with the given name
     * should NOT be considered "available", meaning that the queue of at least
     * one Connection that contain this Relationship is full. This is generally
     * used only when dealing with Processors that use the
     * {@link nifi.processor.annotation.TriggerWhenAnyDestinationAvailable}
     *
     * @param relationshipName name of relationship.
     */
    void setRelationshipUnavailable(String relationshipName);

    /**
     * Adds the given {@link ControllerService} to this TestRunner so that the
     * configured Processor can access it using the given
     * <code>identifier</code>. The ControllerService is not expected to be
     * initialized, as the framework will create the appropriate
     * {@link nifi.controller.ControllerServiceInitializationContext ControllerServiceInitializationContext}
     * and initialize the ControllerService with no specified properties.
     *
     * This will call any method on the given Controller Service that is
     * annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnAdded @OnAdded} annotation.
     *
     * @param identifier of service
     * @param service the service
     * @throws InitializationException ie
     */
    void addControllerService(String identifier, ControllerService service) throws InitializationException;

    /**
     * Adds the given {@link ControllerService} to this TestRunner so that the
     * configured Processor can access it using the given
     * <code>identifier</code>. The ControllerService is not expected to be
     * initialized, as the framework will create the appropriate
     * {@link nifi.controller.ControllerServiceInitializationContext ControllerServiceInitializationContext}
     * and initialize the ControllerService with the given properties.
     *
     * This will call any method on the given Controller Service that is
     * annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnAdded @OnAdded} annotation.
     *
     * @param identifier of service
     * @param service the service
     * @param properties service properties
     * @throws InitializationException ie
     */
    void addControllerService(String identifier, ControllerService service, Map<String, String> properties) throws InitializationException;

    /**
     * <p>
     * Marks the Controller Service as enabled so that it can be used by other
     * components.
     * </p>
     *
     * <p>
     * This method will result in calling any method in the Controller Service
     * that is annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnEnabled @OnEnabled}
     * annotation.
     * </p>
     *
     * @param service the service to enable
     */
    void enableControllerService(ControllerService service);

    /**
     * <p>
     * Marks the Controller Service as disabled so that it cannot be used by
     * other components.
     * </p>
     *
     * <p>
     * This method will result in calling any method in the Controller Service
     * that is annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnDisabled @OnDisabled}
     * annotation.
     * </p>
     *
     * @param service the service to disable
     */
    void disableControllerService(ControllerService service);

    /**
     * @param service the service
     * @return {@code true} if the given Controller Service is enabled,
     * {@code false} if it is disabled
     *
     * @throws IllegalArgumentException if the given ControllerService is not
     * known by this TestRunner (i.e., it has not been added via the
     * {@link #addControllerService(String, ControllerService)} or
     * {@link #addControllerService(String, ControllerService, Map)} method or
     * if the Controller Service has been removed via the
     * {@link #removeControllerService(ControllerService)} method.
     */
    boolean isControllerServiceEnabled(ControllerService service);

    /**
     * <p>
     * Removes the Controller Service from the TestRunner. This will call any
     * method on the ControllerService that is annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnRemoved @OnRemoved}
     * annotation.
     * </p>
     *
     * @param service the service
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     * known by this TestRunner (i.e., it has not been added via the
     * {@link #addControllerService(String, ControllerService)} or
     * {@link #addControllerService(String, ControllerService, Map)} method or
     * if the Controller Service has been removed via the
     * {@link #removeControllerService(ControllerService)} method.
     *
     */
    void removeControllerService(ControllerService service);

    /**
     * Sets the given property on the given ControllerService
     *
     * @param service to modify
     * @param property to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     * known by this TestRunner (i.e., it has not been added via the
     * {@link #addControllerService(String, ControllerService)} or
     * {@link #addControllerService(String, ControllerService, Map)} method or
     * if the Controller Service has been removed via the
     * {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, PropertyDescriptor property, String value);

    /**
     * Sets the given property on the given ControllerService
     *
     * @param service to modify
     * @param property to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     * known by this TestRunner (i.e., it has not been added via the
     * {@link #addControllerService(String, ControllerService)} or
     * {@link #addControllerService(String, ControllerService, Map)} method or
     * if the Controller Service has been removed via the
     * {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, PropertyDescriptor property, AllowableValue value);

    /**
     * Sets the property with the given name on the given ControllerService
     *
     * @param service to modify
     * @param propertyName to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     * known by this TestRunner (i.e., it has not been added via the
     * {@link #addControllerService(String, ControllerService)} or
     * {@link #addControllerService(String, ControllerService, Map)} method or
     * if the Controller Service has been removed via the
     * {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, String propertyName, String value);

    /**
     * Sets the annontation data of the given service to the provided annotation
     * data.
     *
     * @param service to modify
     * @param annotationData the data
     *
     * @throws IllegalStateException if the Controller Service is not disabled
     *
     * @throws IllegalArgumentException if the given ControllerService is not
     * known by this TestRunner (i.e., it has not been added via the
     * {@link #addControllerService(String, ControllerService)} or
     * {@link #addControllerService(String, ControllerService, Map)} method or
     * if the Controller Service has been removed via the
     * {@link #removeControllerService(ControllerService)} method.
     */
    void setAnnotationData(ControllerService service, String annotationData);

    /**
     * @param identifier of controller service
     * @return the {@link ControllerService} that is registered with the given
     * identifier, or <code>null</code> if no Controller Service exists with the
     * given identifier
     */
    ControllerService getControllerService(String identifier);

    /**
     * Assert that the currently configured set of properties/annotation data
     * are valid for the given Controller Service.
     *
     * @param service the service to validate
     * @throws IllegalArgumentException if the given ControllerService is not
     * known by this TestRunner (i.e., it has not been added via the
     * {@link #addControllerService(String, ControllerService)} or
     * {@link #addControllerService(String, ControllerService, Map)} method or
     * if the Controller Service has been removed via the
     * {@link #removeControllerService(ControllerService)} method.
     */
    void assertValid(ControllerService service);

    /**
     * Assert that the currently configured set of properties/annotation data
     * are NOT valid for the given Controller Service.
     *
     * @param service the service to validate
     * @throws IllegalArgumentException if the given ControllerService is not
     * known by this TestRunner (i.e., it has not been added via the
     * {@link #addControllerService(String, ControllerService)} or
     * {@link #addControllerService(String, ControllerService, Map)} method or
     * if the Controller Service has been removed via the
     * {@link #removeControllerService(ControllerService)} method.
     *
     */
    void assertNotValid(ControllerService service);

    /**
     * @param <T> type of service
     * @param identifier identifier of service
     * @param serviceType type of service
     * @return the {@link ControllerService} that is registered with the given
     * identifier, cast as the provided service type, or <code>null</code> if no
     * Controller Service exists with the given identifier
     *
     * @throws ClassCastException if the identifier given is registered for a
     * Controller Service but that Controller Service is not of the type
     * specified
     */
    <T extends ControllerService> T getControllerService(String identifier, Class<T> serviceType);

    /**
     * Specifies whether or not the TestRunner will validate the use of
     * Expression Language. By default, the value is <code>true</code>, which
     * means that an Exception will be thrown if the Processor attempts to
     * obtain the configured value of a Property without calling
     * {@link nifi.components.PropertyValue#evaluateAttributeExpressions evaluateAttributeExpressions}
     * on the Property Value or if
     * {@link nifi.components.PropertyValue#evaluateAttributeExpressions evaluateAttributeExpressions}
     * is called but the PropertyDescriptor indicates that the Expression
     * Language is not supported.
     *
     * <p>
     * <b>See Also:
     * </b>{@link PropertyDescriptor.Builder#expressionLanguageSupported(boolean)}
     * </p>
     *
     * @param validate whether there is any need to validate the EL was used
     */
    void setValidateExpressionUsage(boolean validate);

    /**
     * Removes the {@link PropertyDescriptor} from the {@link ProcessContext},
     * effectively setting its value to null.
     *
     * @param descriptor of property to remove
     * @return true if removed
     */
    boolean removeProperty(PropertyDescriptor descriptor);

    /**
     * Returns a {@link List} of all {@link ProvenanceEventRecord}s that were
     * emitted by the Processor
     *
     * @return a List of all Provenance Events that were emitted by the
     *         Processor
     */
    List<ProvenanceEventRecord> getProvenanceEvents();

    /**
     * Clears the Provenance Events that have been emitted by the Processor
     */
    void clearProvenanceEvents();
}
