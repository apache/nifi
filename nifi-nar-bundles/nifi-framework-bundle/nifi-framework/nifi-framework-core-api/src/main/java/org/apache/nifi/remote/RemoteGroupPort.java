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
package org.apache.nifi.remote;

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;

public abstract class RemoteGroupPort extends AbstractPort implements Port, RemoteDestination {

    public RemoteGroupPort(String id, String name, ProcessGroup processGroup, ConnectableType type, ProcessScheduler scheduler) {
        super(id, name, processGroup, type, scheduler);
    }

    public abstract RemoteProcessGroup getRemoteProcessGroup();

    public abstract TransferDirection getTransferDirection();

    @Override
    public abstract boolean isUseCompression();

    public abstract void setUseCompression(boolean useCompression);

    public abstract boolean getTargetExists();

    public abstract boolean isTargetRunning();

    public abstract Integer getBatchCount();

    public abstract void setBatchCount(Integer batchCount);

    public abstract String getBatchSize();

    public abstract void setBatchSize(String batchSize);

    public abstract String getBatchDuration();

    public abstract void setBatchDuration(String batchDuration);
}
