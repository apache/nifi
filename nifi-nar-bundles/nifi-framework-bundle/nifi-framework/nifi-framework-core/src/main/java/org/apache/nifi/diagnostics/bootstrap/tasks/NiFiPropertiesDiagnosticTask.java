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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.util.NiFiProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NiFiPropertiesDiagnosticTask implements DiagnosticTask {
    private static final List<String> PROPERTY_NAMES = Arrays.asList(
        "nifi.cluster.protocol.heartbeat.interval",
        "nifi.cluster.node.connection.timeout",
        "nifi.cluster.node.read.timeout",
        "nifi.zookeeper.connect.timeout",
        "nifi.zookeeper.session.timeout",
        "nifi.ui.autorefresh.interval",
        "nifi.cluster.node.protocol.max.threads",
        "nifi.cluster.node.protocol.threads",
        "nifi.security.allow.anonymous.authentication",
        "nifi.security.user.login.identity.provider",
        "nifi.security.user.authorizer",
        "nifi.provenance.repository.implementation",
        "nifi.provenance.repository.index.shard.size",
        "nifi.provenance.repository.max.storage.size",
        "nifi.components.status.repository.buffer.size",
        "nifi.components.status.snapshot.frequency",
        "nifi.content.repository.archive.max.retention.period",
        "nifi.content.repository.archive.max.usage.percentage",
        "nifi.flowfile.repository.checkpoint.interval",
        "nifi.flowfile.repository.always.sync",
        "nifi.components.status.snapshot.frequency",
        "nifi.bored.yield.duration",
        "nifi.queue.swap.threshold",
        "nifi.security.identity.mapping.pattern.dn",
        "nifi.security.identity.mapping.value.dn",
        "nifi.security.identity.mapping.transform.dn",
        "nifi.security.identity.mapping.pattern.kerb",
        "nifi.security.identity.mapping.value.kerb",
        "nifi.security.identity.mapping.transform.kerb",
        "nifi.security.group.mapping.pattern.anygroup",
        "nifi.security.group.mapping.value.anygroup",
        "nifi.security.group.mapping.transform.anygroup"
    );

    private final NiFiProperties nifiProperties;

    public NiFiPropertiesDiagnosticTask(final NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        for (final String propertyName : PROPERTY_NAMES) {
            details.add(propertyName + " : " + nifiProperties.getProperty(propertyName));
        }

        return new StandardDiagnosticsDumpElement("NiFi Properties", details);
    }
}
