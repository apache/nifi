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

import java.io.Closeable;
import java.util.Properties;
import java.util.Set;

public interface ConfigurationChangeNotifier extends Closeable {

    /**
     * Provides an opportunity for the implementation to perform configuration and initialization based on properties received from the bootstrapping configuration
     *
     * @param properties from the bootstrap configuration
     */
    void initialize(Properties properties);

    /**
     * Begins the associated notification service provided by the given implementation.  In most implementations, no action will occur until this method is invoked.
     */
    void start();

    /**
     * Provides an immutable collection of listeners for the notifier instance
     *
     * @return a collection of those listeners registered for notifications
     */
    Set<ConfigurationChangeListener> getChangeListeners();

    /**
     * Adds a listener to be notified of configuration changes
     *
     * @param listener to be added to the collection
     * @return true if the listener was added; false if already registered
     */
    boolean registerListener(ConfigurationChangeListener listener);

    /**
     * Provide the mechanism by which listeners are notified
     */
    void notifyListeners();

}
