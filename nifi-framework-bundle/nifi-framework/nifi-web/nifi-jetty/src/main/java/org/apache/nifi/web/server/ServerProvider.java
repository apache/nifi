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
package org.apache.nifi.web.server;

import org.apache.nifi.util.NiFiProperties;
import org.eclipse.jetty.server.Server;

/**
 * Abstraction for configuring Server instances based on application properties
 */
interface ServerProvider {
    /**
     * Get Server configured using application properties
     *
     * @param properties Application properties
     * @return Server
     */
    Server getServer(NiFiProperties properties);
}
