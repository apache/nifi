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
package org.apache.nifi.services.smb;

import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public interface SmbClientProvider {

    /**
     * Returns the identifier of the service location.
     *
     * @return the remote location
     */
    default URI getServiceLocation() {
        return getServiceLocation(Map.of());
    }

    /**
     * Returns the identifier of the service location.
     *
     * @param attributes FlowFile attributes to evaluate connection properties
     * @return the remote location
     */
    URI getServiceLocation(Map<String, String> attributes);

    /**
     * Returns the smb client to use.
     *
     * @return the client.
     */
    default SmbClientService getClient(ComponentLog logger) throws IOException {
        return getClient(logger, Map.of());
    }

    /**
     * Returns the smb client to use.
     *
     * @param attributes FlowFile attributes to evaluate connection properties
     * @return the client.
     */
    SmbClientService getClient(ComponentLog logger, Map<String, String> attributes) throws IOException;

}
