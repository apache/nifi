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

package org.apache.nifi.components.state;

import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.logging.ComponentLog;

/**
 * This interface defines an initialization context that is passed to a {@link StateProvider} when it
 * is initialized.
 */
public interface StateProviderInitializationContext {
    /**
     * @return the identifier if the StateProvider
     */
    String getIdentifier();

    /**
     * @return a Map of Property Descriptors to their configured values
     */
    Map<PropertyDescriptor, PropertyValue> getProperties();

    /**
     * Returns the configured value for the given property
     *
     * @param property the property to retrieve the value for
     *
     * @return the configured value for the property.
     */
    PropertyValue getProperty(PropertyDescriptor property);

    /**
     * @return the SSL Context that should be used to communicate with remote resources,
     *         or <code>null</code> if no SSLContext has been configured
     */
    SSLContext getSSLContext();

    /**
     * @return the logger for the given state provider
     */
    ComponentLog getLogger();

}
