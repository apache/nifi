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

package org.apache.nifi.minifi.c2.api;

import java.util.List;
import java.util.Map;

/**
 * A configuration provider is capable of taking a parameter map and returning a configuration with a given content type
 */
public interface ConfigurationProvider {
    /**
     * Gets the content type that this provider returns
     *
     * @return the content type that this provider returns
     */
    String getContentType();

    /**
     * Gets the configuration that corresponds to the passed in parameters
     *
     * @param version the version of the configuration to get
     * @param parameters the parameters passed in by the client (please note that these are provided by a client and should NOT be trusted to be sanitized)
     * @return an input stream of the configuration
     * @throws ConfigurationProviderException if there is an error in the configuration
     */
    Configuration getConfiguration(Integer version, Map<String, List<String>> parameters) throws ConfigurationProviderException;
}
