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
package org.apache.nifi.groups;

public interface RemoteProcessGroupPortDescriptor {

    /**
     * The comments as configured in the target port.
     *
     * @return
     */
    String getComments();

    /**
     * The number tasks that may transmit flow files to the target port
     * concurrently.
     *
     * @return
     */
    Integer getConcurrentlySchedulableTaskCount();

    /**
     * The id of the target port.
     *
     * @return
     */
    String getId();

    /**
     * The id of the remote process group that this port resides in.
     *
     * @return
     */
    String getGroupId();

    /**
     * The name of the target port.
     *
     * @return
     */
    String getName();

    /**
     * Whether or not this remote group port is configured for transmission.
     *
     * @return
     */
    Boolean isTransmitting();

    /**
     * Whether or not flow file are compressed when sent to this target port.
     *
     * @return
     */
    Boolean getUseCompression();

    /**
     * Whether ot not the target port exists.
     *
     * @return
     */
    Boolean getExists();

    /**
     * Whether or not the target port is running.
     *
     * @return
     */
    Boolean isTargetRunning();

    /**
     * Whether or not this port has either an incoming or outgoing connection.
     *
     * @return
     */
    Boolean isConnected();

}
