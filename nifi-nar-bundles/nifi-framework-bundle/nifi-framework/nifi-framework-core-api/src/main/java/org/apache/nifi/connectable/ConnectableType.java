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
package org.apache.nifi.connectable;

import javax.xml.bind.annotation.XmlEnum;

@XmlEnum
public enum ConnectableType {

    PROCESSOR,
    /**
     * Port that lives within an RemoteProcessGroup and is used to send data to
     * remote NiFi instances
     */
    REMOTE_INPUT_PORT,
    /**
     * Port that lives within a RemoteProcessGroup and is used to receive data
     * from remote NiFi instances
     */
    REMOTE_OUTPUT_PORT,
    /**
     * Root Group Input Ports and Local Input Ports
     */
    INPUT_PORT,
    /**
     * Root Group Output Ports and Local Output Ports
     */
    OUTPUT_PORT,
    FUNNEL
}
