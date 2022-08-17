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
package org.apache.nifi.flow.resource;

/**
 * Responsible for polling one external source using {@code org.apache.nifi.flow.resource.ExternalResourceProvider}
 */
interface ExternalResourceProviderWorker extends Runnable {

    /**
     * @return Returns the name of the worker.
     */
    String getName();

    /**
     * @return Returns the enveloped provider.
     */
    ExternalResourceProvider getProvider();

    /**
     * @return Returns true in case the worker is running.
     */
    boolean isRunning();

    /**
     * Stops the worker. The stop might not happen instantly but the implementation must guarantee the worker stops polling within a reasonable amount of time.
     */
    void stop();
}
