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

package org.apache.nifi.bootstrap;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.BiConsumer;

public interface BootstrapCommunicator {

    /**
     * Sends a command with specific arguments to the bootstrap process
     *
     * @param command the command to send
     * @param args    the args to send
     * @return {@link CommandResult} of the command sent to Bootstrap
     * @throws IOException exception in case of communication issue
     */
    CommandResult sendCommand(String command, String... args) throws IOException;

    /**
     * Register a handler for messages coming from bootstrap process
     * @param command the command
     * @param handler handler for the specific command
     */
    void registerMessageHandler(String command, BiConsumer<String[], OutputStream> handler);
}
