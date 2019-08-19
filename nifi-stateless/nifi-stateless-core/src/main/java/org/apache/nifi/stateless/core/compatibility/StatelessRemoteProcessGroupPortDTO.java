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
package org.apache.nifi.stateless.core.compatibility;

import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;

public class StatelessRemoteProcessGroupPortDTO implements StatelessRemoteProcessGroupPort {

    private final RemoteProcessGroupPortDTO remotePort;

    public StatelessRemoteProcessGroupPortDTO(final RemoteProcessGroupPortDTO port) {
        this.remotePort = port;
    }

    @Override
    public String getId() {
        return this.remotePort.getId();
    }

    @Override
    public String getGroupId() {
        return this.remotePort.getGroupId();
    }

    @Override
    public String getName() {
        return this.remotePort.getName();
    }

    @Override
    public Boolean getUseCompression() {
        return this.remotePort.getUseCompression();
    }

    @Override
    public Integer getBatchSizeCount() {
        return this.remotePort.getBatchSettings().getCount();
    }

    @Override
    public String getBatchSizeDuration() {
        return this.remotePort.getBatchSettings().getDuration();
    }

    @Override
    public String getBatchSizeSize() {
        return this.remotePort.getBatchSettings().getSize();
    }
}
