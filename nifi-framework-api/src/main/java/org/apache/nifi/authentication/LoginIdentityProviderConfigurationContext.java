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
package org.apache.nifi.authentication;

import java.util.Map;
import org.apache.nifi.configuration.NonComponentConfigurationContext;

/**
 *
 */
public interface LoginIdentityProviderConfigurationContext extends NonComponentConfigurationContext {

    /**
     * @return identifier for the authority provider
     */
    @Override
    String getIdentifier();

    /**
     * Retrieves all properties the component currently understands regardless
     * of whether a value has been set for them or not. If no value is present
     * then its value is null and thus any registered default for the property
     * descriptor applies.
     *
     * @return Map of all properties
     */
    @Override
    Map<String, String> getProperties();

    /**
     * Returns the value of the provided property. This method does not substitute default values, so the value returned will be {@code null} if not set.
     *
     * @param property the property to retrieve
     * @return the current property value (can be null)
     */
    @Override
    String getProperty(String property);
}
