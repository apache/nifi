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
package org.apache.nifi.nar;

/**
 * Represents an external source where the NAR files might be acquired from. Used by the NAR auto loader functionality
 * in order to poll an external source for new NAR files to load.
 */
public interface NarAutoLoaderExternalSource {
    /**
     * Starts the necessary resources. This might be a connection or other resource based on the nature of the external source.
     *
     * @param context Context which might contain setup or runtime information.
     */
    void start(NarAutoLoaderContext context);

    /**
     * Stops the used resources.
     */
    void stop();

    /**
     * Polls the external source and if it finds NAR files not already loaded, the service acquires the files in order to load them.
     */
    void acquire();
}
