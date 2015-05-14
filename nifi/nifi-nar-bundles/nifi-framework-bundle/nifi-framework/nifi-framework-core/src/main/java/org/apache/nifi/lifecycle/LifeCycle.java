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
package org.apache.nifi.lifecycle;

/**
 * Represents a start/stop lifecyle for a component.  <code>start</code> should only be called once per lifecyle unless otherwise documented by implementing classes.
 *
 */
public interface LifeCycle {

    /**
     * Initiates the start state of the lifecyle. Should not throw an exception if the component is already running.
     *
     * @throws LifeCycleStartException if startup or initialization failed
     */
    void start() throws LifeCycleStartException;

    /**
     * Initiates the stop state of the lifecycle. Should not throw an exception if the component is already stopped.
     *
     * @param force true if all efforts such as thread interruption should be attempted to stop the component; false if a graceful stopping should be employed
     *
     * @throws LifeCycleStopException if the shutdown failed
     */
    void stop(boolean force) throws LifeCycleStopException;

    /**
     * @return true if the component is started, but not yet stopped; false otherwise
     */
    boolean isRunning();

}
