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
package org.apache.nifi;

import org.apache.nifi.runtime.StandardUncaughtExceptionHandler;
import org.apache.nifi.runtime.Application;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Application main class
 */
public class NiFi {
    /**
     * Start Application after initializing logging and uncaught exception handling
     *
     * @param arguments Application arguments are ignored
     */
    public static void main(final String[] arguments) {
        // Install JUL SLF4J Bridge logging before starting application
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        // Set Uncaught Exception Handler before other operations
        Thread.setDefaultUncaughtExceptionHandler(new StandardUncaughtExceptionHandler());

        // Run Application
        final Runnable applicationCommand = new Application();
        applicationCommand.run();
    }
}
