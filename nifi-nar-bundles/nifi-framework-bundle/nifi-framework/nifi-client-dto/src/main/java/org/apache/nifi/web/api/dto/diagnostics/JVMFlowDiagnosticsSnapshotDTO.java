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

package org.apache.nifi.web.api.dto.diagnostics;

import java.util.HashSet;
import java.util.Set;

import jakarta.xml.bind.annotation.XmlType;

import org.apache.nifi.web.api.dto.BundleDTO;

import io.swagger.v3.oas.annotations.media.Schema;

@XmlType(name = "jvmFlowDiagnosticsSnapshot")
public class JVMFlowDiagnosticsSnapshotDTO implements Cloneable {
    private String uptime;
    private String timeZone;
    private Integer activeTimerDrivenThreads;
    private Set<BundleDTO> bundlesLoaded;

    @Schema(description = "How long this node has been running, formatted as hours:minutes:seconds.milliseconds")
    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    @Schema(description = "The name of the Time Zone that is configured, if available")
    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }


    @Schema(description = "The number of timer-driven threads that are active")
    public Integer getActiveTimerDrivenThreads() {
        return activeTimerDrivenThreads;
    }

    public void setActiveTimerDrivenThreads(Integer activeTimerDrivenThreads) {
        this.activeTimerDrivenThreads = activeTimerDrivenThreads;
    }

    @Schema(description = "The NiFi Bundles (NARs) that are loaded by NiFi")
    public Set<BundleDTO> getBundlesLoaded() {
        return bundlesLoaded;
    }

    public void setBundlesLoaded(Set<BundleDTO> bundlesLoaded) {
        this.bundlesLoaded = bundlesLoaded;
    }

    @Override
    public JVMFlowDiagnosticsSnapshotDTO clone() {
        final JVMFlowDiagnosticsSnapshotDTO clone = new JVMFlowDiagnosticsSnapshotDTO();
        clone.activeTimerDrivenThreads = activeTimerDrivenThreads;
        clone.bundlesLoaded = bundlesLoaded == null ? null : new HashSet<>(bundlesLoaded);
        clone.timeZone = timeZone;
        clone.uptime = uptime;

        return clone;
    }
}
