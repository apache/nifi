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
package org.apache.nifi.processor;

import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerServiceLookup;

/**
 * <p>
 * Provides a bridge between a Processor and the NiFi Framework
 * </p>
 *
 * <p>
 * <b>Note: </b>Implementations of this interface are NOT necessarily
 * thread-safe.
 * </p>
 */
public interface ProcessContext {

    /**
     * Retrieves the current value set for the given descriptor, if a value is
     * set - else uses the descriptor to determine the appropriate default value
     *
     * @param descriptor
     * @return
     */
    PropertyValue getProperty(PropertyDescriptor descriptor);

    /**
     * Retrieves the current value set for the given descriptor, if a value is
     * set - else uses the descriptor to determine the appropriate default value
     *
     * @param propertyName
     * @return
     */
    PropertyValue getProperty(String propertyName);

    /**
     * Creates and returns a {@link PropertyValue} object that can be used for
     * evaluating the value of the given String
     *
     * @param rawValue
     * @return
     */
    PropertyValue newPropertyValue(String rawValue);

    /**
     * <p>
     * Causes the Processor not to be scheduled for some pre-configured amount
     * of time. The duration of time for which the processor will not be
     * scheduled is configured in the same manner as the processor's scheduling
     * period.
     * </p>
     *
     * <p>
     * <b>Note: </b>This is NOT a blocking call and does not suspend execution
     * of the current thread.
     * </p>
     */
    void yield();

    /**
     * @return the maximum number of threads that may be executing this
     * processor's code at any given time
     */
    int getMaxConcurrentTasks();

    /**
     * @return the annotation data configured for this processor
     */
    String getAnnotationData();

    /**
     * Returns a Map of all PropertyDescriptors to their configured values. This
     * Map may or may not be modifiable, but modifying its values will not
     * change the values of the processor's properties
     *
     * @return
     */
    Map<PropertyDescriptor, String> getProperties();

    /**
     * Encrypts the given value using the password provided in the NiFi
     * Properties
     *
     * @param unencrypted
     * @return
     */
    String encrypt(String unencrypted);

    /**
     * Decrypts the given value using the password provided in the NiFi
     * Properties
     *
     * @param encrypted
     * @return
     */
    String decrypt(String encrypted);

    /**
     * Provides a {@code ControllerServiceLookup} that can be used to obtain a
     * Controller Service
     *
     * @return
     */
    ControllerServiceLookup getControllerServiceLookup();
}
