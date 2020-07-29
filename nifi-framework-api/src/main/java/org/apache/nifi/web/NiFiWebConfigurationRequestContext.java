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
package org.apache.nifi.web;

/**
 * Contextual details required to make a configuration request from a UI
 * extension.
 */
public interface NiFiWebConfigurationRequestContext extends NiFiWebRequestContext {

    /**
     * The revision to include in the request.
     *
     * @return the revision
     */
    Revision getRevision();

    /**
     * Returns whether the node disconnection is acknowledged. By acknowledging disconnection, the
     * user is able to modify the flow on a disconnected node.
     *
     * @return whether the disconnection is acknowledged
     */
    default boolean isDisconnectionAcknowledged() {
        return false;
    }

}
