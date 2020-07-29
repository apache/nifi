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
package org.apache.nifi.processors.gcp.credentials.factory;

import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Specifies a strategy for validating and creating GCP credentials from a list of properties configured on a
 * Processor, Controller Service, Reporting Service, or other component.  Supports only primary credentials like
 * default credentials or API keys.
 */
public interface CredentialsStrategy {

    /**
     * Name of the strategy, suitable for displaying to a user in validation messages.
     * @return strategy name
     */
    String getName();

    /**
     * Determines if this strategy can create primary credentials using the given properties.
     * @return true if primary credentials can be created
     */
    boolean canCreatePrimaryCredential(Map<PropertyDescriptor, String> properties);


    /**
     * Validates the properties belonging to this strategy, given the selected primary strategy.  Errors may result
     * from individually malformed properties, invalid combinations of properties, or inappropriate use of properties
     * not consistent with the primary strategy.
     * @param primaryStrategy the prevailing primary strategy
     * @return validation errors
     */
    Collection<ValidationResult> validate(ValidationContext validationContext, CredentialsStrategy primaryStrategy);

    /**
     * Creates an AuthCredentials instance for this strategy, given the properties defined by the user.
     * @param transportFactory Sub-classes should utilize this transport factory
     *                        to support common network related configs such as proxy
     * @throws IOException if the provided credentials cannot be accessed or are invalid
     */
    GoogleCredentials getGoogleCredentials(Map<PropertyDescriptor, String> properties, HttpTransportFactory transportFactory) throws IOException;
}
