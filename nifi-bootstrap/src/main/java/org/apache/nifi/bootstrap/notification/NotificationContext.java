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
package org.apache.nifi.bootstrap.notification;

import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;

public interface NotificationContext {

    /**
     * Returns the configured value for the given PropertyDescriptor. Note that the implementation
     * of PropertyValue will throw an Exception if calling {@link PropertyValue#asControllerService()} or
     * {@link PropertyValue#asControllerService(Class)}, as Controller Services are not allowed to be
     * referenced by Notification Services
     *
     * @param descriptor the property whose value should be returned
     * @return the configured value for the given PropertyDescriptor, or the default
     *         value for the PropertyDescriptor if no value has been configured
     */
    PropertyValue getProperty(PropertyDescriptor descriptor);

    /**
     * @return a Map of all PropertyDescriptors to their configured values. This
     *         Map may or may not be modifiable, but modifying its values will not
     *         change the values of the processor's properties
     */
    Map<PropertyDescriptor, String> getProperties();
}
