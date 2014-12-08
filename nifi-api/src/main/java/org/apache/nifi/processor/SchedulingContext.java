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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerServiceLookup;

public interface SchedulingContext extends ProcessContext {

    /**
     * <p>
     * Indicates to the framework that the Controller Service with the given
     * identifier will be used by this Processor. This will prevent any Data
     * Flow Manager from disabling the Controller Service while this Processor
     * is still running.
     * </p>
     *
     * <p>
     * Generally, a Controller Service is accessed by indicating in a
     * {@link PropertyDescriptor} that the PropertyDescriptor identifies a
     * Controller Service via the
     * {@link PropertyDescriptor.Builder#identifiesControllerService(Class) identifiesControllerService(Class)}
     * method and then calling
     * {@link ProcessContext#getProperty(PropertyDescriptor)}.{@link PropertyValue#asControllerService(Class) asControllerService(Class)}.
     * In this case, it is not necessary to lease the Controller Service, as the
     * Framework will handle this.
     * </p>
     *
     * <p>
     * There are, however, cases in which a Controller Service must be accessed
     * in a different way, via the {@link ControllerServiceLookup} (accessed via
     * {@link ProcessContext#getControllerServiceLookup()}). In this case, the
     * Controller Service that is obtained from the ControllerServiceLookup can
     * be disabled by a Data Flow Manager while it is still in use by a
     * Processor, causing IllegalStateException to be thrown whenever the
     * Processor attempts to interact with the service. This method provides a
     * mechanism by which a Processor is able to indicate that the Controller
     * Service with the given identifier should not be disabled while this
     * Processor is running.
     * </p>
     *
     * <p>
     * For any Controller Service that is leased by calling this method, the
     * lease will automatically be terminated, allowing the Controller Service
     * to be disabled, whenever the Processor is stopped.
     * </p>
     *
     * @param identifier
     *
     * @throws IllegalArgumentException if no Controller Service exists with the
     * given identifier
     */
    void leaseControllerService(String identifier);

}
