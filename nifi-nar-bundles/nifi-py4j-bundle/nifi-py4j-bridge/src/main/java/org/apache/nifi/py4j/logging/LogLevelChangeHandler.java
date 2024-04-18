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
package org.apache.nifi.py4j.logging;

/**
 * Handler abstraction for registering Listeners and invoking Listeners on level changes
 */
public interface LogLevelChangeHandler extends LogLevelChangeListener {
    /**
     * Register Log Level Change Listener with provided identifier
     *
     * @param identifier Tracking identifier associated with Log Level Change Listener
     * @param listener Log Level Change Listener to be registered
     */
    void addListener(String identifier, LogLevelChangeListener listener);

    /**
     * Remove registered listener based on provided identifier
     *
     * @param identifier Tracking identifier of registered listener to be removed
     */
    void removeListener(String identifier);
}
