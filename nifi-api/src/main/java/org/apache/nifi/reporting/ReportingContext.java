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
package org.apache.nifi.reporting;

import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerServiceLookup;

/**
 * This interface provides a bridge between the NiFi Framework and a
 * {@link ReportingTask}. This context allows a ReportingTask to access
 * statistics, metrics, and monitoring information, as well as configuration
 * supplied by the user.
 */
public interface ReportingContext {

    /**
     * Returns a Map of all known {@link PropertyDescriptor}s to their
     * configured properties. This Map will contain a <code>null</code> for any
     * Property that has not been configured by the user, even if the
     * PropertyDescriptor has a default value.
     *
     * @return
     */
    Map<PropertyDescriptor, String> getProperties();

    /**
     * A PropertyValue that represents the user-configured value for the given
     * {@link PropertyDescriptor}.
     *
     * @param propertyName
     * @return
     */
    PropertyValue getProperty(PropertyDescriptor propertyName);

    /**
     * Returns the {@link EventAccess} object that can be used to obtain
     * information about specific events and reports that have happened
     *
     * @return
     */
    EventAccess getEventAccess();

    /**
     * Returns the {@link BulletinRepository} that can be used to analyze
     * Bulletins that have been emitted and register new Bulletins
     *
     * @return
     */
    BulletinRepository getBulletinRepository();

    /**
     * Creates a system-level {@link Bulletin} with the given category, severity
     * level, and message, so that the Bulletin can be added to the
     * {@link BulletinRepository}.
     *
     * @param category
     * @param severity
     * @param message
     * @return
     */
    Bulletin createBulletin(String category, Severity severity, String message);

    /**
     * Creates a {@link Bulletin} for the component with the specified
     * identifier
     *
     * @param componentId the ID of the component
     * @param category the name of the bulletin's category
     * @param severity the severity level of the bulletin
     * @param message the bulletin's message
     * @return
     */
    Bulletin createBulletin(String componentId, String category, Severity severity, String message);

    /**
     * Returns the {@link ControllerServiceLookup} which can be used to obtain
     * Controller Services
     *
     * @return
     */
    ControllerServiceLookup getControllerServiceLookup();
}
