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
package org.apache.nifi.toolkit.cli.impl.client.nifi;

import org.apache.nifi.web.api.entity.PortEntity;

import java.io.IOException;

public interface OutputPortClient {

    PortEntity createOutputPort(String parentGroupId, PortEntity entity) throws NiFiClientException, IOException;

    PortEntity getOutputPort(String id) throws NiFiClientException, IOException;

    PortEntity updateOutputPort(PortEntity entity) throws NiFiClientException, IOException;

    PortEntity deleteOutputPort(PortEntity entity) throws NiFiClientException, IOException;

    /**
     * @deprecated use startOutputPort
     */
    @Deprecated
    PortEntity startInpuOutputPort(PortEntity entity) throws NiFiClientException, IOException;

    PortEntity startOutputPort(PortEntity entity) throws NiFiClientException, IOException;

    PortEntity stopOutputPort(PortEntity entity) throws NiFiClientException, IOException;
}
