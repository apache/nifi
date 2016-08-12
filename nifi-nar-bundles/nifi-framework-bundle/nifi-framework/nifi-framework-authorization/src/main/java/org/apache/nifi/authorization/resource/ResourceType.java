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
package org.apache.nifi.authorization.resource;

public enum ResourceType {
    Controller("/controller"),
    ControllerService("/controller-services"),
    Counters("/counters"),
    Funnel("/funnels"),
    Flow("/flow"),
    InputPort("/input-ports"),
    Label("/labels"),
    OutputPort("/output-ports"),
    Policy("/policies"),
    Processor("/processors"),
    ProcessGroup("/process-groups"),
    Provenance("/provenance"),
    Data("/data"),
    Proxy("/proxy"),
    RemoteProcessGroup("/remote-process-groups"),
    ReportingTask("/reporting-tasks"),
    Resource("/resources"),
    SiteToSite("/site-to-site"),
    DataTransfer("/data-transfer"),
    System("/system"),
    Template("/templates"),
    Tenant("/tenants");

    final String value;

    private ResourceType(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ResourceType valueOfValue(final String rawValue) {
        ResourceType type = null;

        for (final ResourceType rt : values()) {
            if (rt.getValue().equals(rawValue)) {
                type = rt;
                break;
            }
        }

        if (type == null) {
            throw new IllegalArgumentException("Unknown resource type value " + rawValue);
        }

        return type;
    }
}