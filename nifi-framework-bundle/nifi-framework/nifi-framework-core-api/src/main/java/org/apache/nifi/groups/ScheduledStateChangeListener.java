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

import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.ScheduledState;

public interface ScheduledStateChangeListener {
    void onScheduledStateChange(ProcessorNode processor, ScheduledState intendedState);

    void onScheduledStateChange(Port port, ScheduledState intendedState);

    void onScheduledStateChange(ControllerServiceNode controllerService, ScheduledState intendedState);

    void onScheduledStateChange(ReportingTaskNode reportingTask, ScheduledState intendedState);

    ScheduledStateChangeListener EMPTY = new ScheduledStateChangeListener() {
        @Override
        public void onScheduledStateChange(ProcessorNode processor, ScheduledState intendedState) {

        }

        @Override
        public void onScheduledStateChange(Port port, ScheduledState intendedState) {

        }

        @Override
        public void onScheduledStateChange(ControllerServiceNode controllerService, ScheduledState intendedState) {

        }

        @Override
        public void onScheduledStateChange(ReportingTaskNode reportingTask, ScheduledState intendedState) {

        }
    };
}
