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

package org.apache.nifi.c2.client.api;

import java.io.IOException;
import java.util.Properties;

/**
 * Helper to support differentiating between config files to recognise changes
 *
 * @param <T> the type of the config files
 */
public interface Differentiator <T> {

    /**
     * Initialise the differentiator with the initial configuration
     *
     * @param properties the properties to be used
     * @param configurationFileHolder holder for the config file
     */
    void initialize(Properties properties, ConfigurationFileHolder configurationFileHolder);

    /**
     * Determine whether the config file changed
     *
     * @param input the conetnt of the new config file
     * @return true if changed and false if not
     * @throws IOException when there is a config file reading related error
     */
    boolean isNew(T input) throws IOException;
}
