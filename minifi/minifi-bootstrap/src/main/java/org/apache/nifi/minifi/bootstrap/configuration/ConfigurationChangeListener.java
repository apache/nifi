/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.minifi.bootstrap.configuration;

import java.io.InputStream;

/**
 * Interface for handling events detected and driven by an associated {@link ConfigurationChangeCoordinator} to which the listener
 * has registered via {@link ConfigurationChangeCoordinator#registerListener(ConfigurationChangeListener)}.
 */
public interface ConfigurationChangeListener {

    /**
     * Provides a mechanism for the implementation to interpret the specified configuration change
     *
     * @param is stream of the detected content received from the change notifier
     */
    void handleChange(InputStream is) throws ConfigurationChangeException;

    /**
     * Returns a succinct string identifying this particular listener
     */
    String getDescriptor();
}
