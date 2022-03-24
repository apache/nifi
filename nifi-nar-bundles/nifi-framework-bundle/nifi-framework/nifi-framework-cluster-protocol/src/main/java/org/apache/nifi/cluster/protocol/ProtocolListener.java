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
package org.apache.nifi.cluster.protocol;

import java.io.IOException;
import java.util.Collection;

import org.apache.nifi.reporting.BulletinRepository;

/**
 * Defines the interface for a listener to process protocol messages.
 *
 */
public interface ProtocolListener {

    /**
     * Starts the instance for listening for messages. Start may only be called
     * if the instance is not running.
     *
     * @throws java.io.IOException ex
     */
    void start() throws IOException;

    /**
     * Stops the instance from listening for messages. Stop may only be called
     * if the instance is running.
     *
     * @throws java.io.IOException ex
     */
    void stop() throws IOException;

    /**
     * @return true if the instance is started; false otherwise.
     */
    boolean isRunning();

    /**
     * @return the handlers registered with the listener
     */
    Collection<ProtocolHandler> getHandlers();

    /**
     * Registers a handler with the listener.
     *
     * @param handler a handler
     */
    void addHandler(ProtocolHandler handler);

    /**
     * Sets the BulletinRepository that can be used to report bulletins
     *
     * @param bulletinRepository repo
     */
    void setBulletinRepository(BulletinRepository bulletinRepository);

    /**
     * Unregisters the handler with the listener.
     *
     * @param handler a handler
     * @return true if the handler was removed; false otherwise
     */
    boolean removeHandler(ProtocolHandler handler);
}
