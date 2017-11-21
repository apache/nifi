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
     * @return comments as configured in the target port
     */
    String getComments();

    /**
     * @return The number tasks that may transmit flow files to the target port
     * concurrently
     */
    Integer getConcurrentlySchedulableTaskCount();

    /**
     * @return id of the target port
     */
    String getId();

    /**
     * @return id of the remote process group that this port resides in
     */
    String getGroupId();

    /**
     * @return name of the target port
     */
    String getName();

    /**
     * @return Whether or not this remote group port is configured for transmission
     */
    Boolean isTransmitting();

    /**
     * @return Whether or not flow file are compressed when sent to this target port
     */
    Boolean getUseCompression();

    /**
     * @return Preferred number of flow files to include in a transaction
     */
    Integer getBatchCount();

    /**
     * @return Preferred number of bytes to include in a transaction
     */
    String getBatchSize();

    /**
     * @return Preferred amount of for a transaction to span
     */
    String getBatchDuration();

    /**
     * @return Whether or not the target port exists
     */
    Boolean getExists();

    /**
     * @return Whether or not the target port is running
     */
    Boolean isTargetRunning();

    /**
     * @return Whether or not this port has either an incoming or outgoing connection
     */
    Boolean isConnected();

}
