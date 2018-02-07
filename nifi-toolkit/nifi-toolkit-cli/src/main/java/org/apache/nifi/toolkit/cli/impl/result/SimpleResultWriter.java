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
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.ResultWriter;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.dto.VariableDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ResultWriter implementation that writes simple human-readable output, primarily for use in the interactive CLI.
 */
public class SimpleResultWriter implements ResultWriter {

    public static String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    @Override
    public void writeBuckets(List<Bucket> buckets, PrintStream output) {
        if (buckets == null || buckets.isEmpty()) {
            return;
        }

        Collections.sort(buckets, Comparator.comparing(Bucket::getName));

        output.println();
        buckets.stream().forEach(b -> writeBucket(b, output));
        output.println();
    }

    @Override
    public void writeBucket(Bucket bucket, PrintStream output) {
        if (bucket == null) {
            return;
        }
        output.println(bucket.getName() + " - " + bucket.getIdentifier());
    }

    @Override
    public void writeFlows(List<VersionedFlow> versionedFlows, PrintStream output) {
        if (versionedFlows == null || versionedFlows.isEmpty()) {
            return;
        }

        Collections.sort(versionedFlows, Comparator.comparing(VersionedFlow::getName));

        output.println();
        versionedFlows.stream().forEach(vf -> writeFlow(vf, output));
        output.println();
    }

    @Override
    public void writeFlow(VersionedFlow versionedFlow, PrintStream output) {
        if (versionedFlow == null) {
            return;
        }
        output.println(versionedFlow.getName() + " - " + versionedFlow.getIdentifier());
    }

    @Override
    public void writeSnapshotMetadata(List<VersionedFlowSnapshotMetadata> versions, PrintStream output) {
        if (versions == null || versions.isEmpty()) {
            return;
        }

        Collections.sort(versions, Comparator.comparing(VersionedFlowSnapshotMetadata::getVersion));

        output.println();
        versions.stream().forEach(vfs -> writeSnapshotMetadata(vfs, output));
        output.println();
    }

    @Override
    public void writeSnapshotMetadata(VersionedFlowSnapshotMetadata version, PrintStream output) {
        if (version == null) {
            return;
        }

        final Date date = new Date(version.getTimestamp());
        final SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT);
        output.println(version.getVersion() + " - " + dateFormatter.format(date) + " - " + version.getAuthor());
    }

    @Override
    public void writeRegistryClients(RegistryClientsEntity clientsEntity, PrintStream output) {
        if (clientsEntity == null) {
            return;
        }

        final Set<RegistryClientEntity> clients = clientsEntity.getRegistries();
        if (clients == null || clients.isEmpty()) {
            return;
        }

        final List<RegistryDTO> registies = clients.stream().map(c -> c.getComponent()).collect(Collectors.toList());
        Collections.sort(registies, Comparator.comparing(RegistryDTO::getName));
        registies.stream().forEach(r -> output.println(r.getName() + " - " + r.getId() + " - " + r.getUri()));
    }

    @Override
    public void writeVariables(VariableRegistryEntity variableRegistryEntity, PrintStream output) {
        if (variableRegistryEntity == null) {
            return;
        }

        final VariableRegistryDTO variableRegistryDTO = variableRegistryEntity.getVariableRegistry();
        if (variableRegistryDTO == null || variableRegistryDTO.getVariables() == null) {
            return;
        }

        final List<VariableDTO> variables = variableRegistryDTO.getVariables().stream().map(v -> v.getVariable()).collect(Collectors.toList());
        Collections.sort(variables, Comparator.comparing(VariableDTO::getName));
        variables.stream().forEach(v -> output.println(v.getName() + " - " + v.getValue()));
    }

    @Override
    public void writeSnapshotMetadata(VersionedFlowSnapshotMetadataSetEntity versionedFlowSnapshotMetadataSetEntity, PrintStream output) {
        if (versionedFlowSnapshotMetadataSetEntity == null) {
            return;
        }

        final Set<VersionedFlowSnapshotMetadataEntity> entities = versionedFlowSnapshotMetadataSetEntity.getVersionedFlowSnapshotMetadataSet();
        if (entities == null || entities.isEmpty()) {
            return;
        }

        final List<VersionedFlowSnapshotMetadata> snapshots = entities.stream().map(v -> v.getVersionedFlowSnapshotMetadata()).collect(Collectors.toList());
        writeSnapshotMetadata(snapshots, output);
    }

    @Override
    public void writeVersionControlInfo(VersionControlInformationEntity versionControlInformationEntity, PrintStream output) {
        if (versionControlInformationEntity == null) {
            return;
        }

        final VersionControlInformationDTO dto = versionControlInformationEntity.getVersionControlInformation();
        if (dto == null) {
            return;
        }

        output.println(dto.getRegistryName() + " - " + dto.getBucketName() + " - " + dto.getFlowName() + " - " + dto.getVersion());
    }

    @Override
    public void writeProcessGroups(List<ProcessGroupEntity> processGroupEntities, PrintStream output) throws IOException {
        if (processGroupEntities == null) {
            return;
        }

        final List<ProcessGroupDTO> dtos = processGroupEntities.stream().map(e -> e.getComponent()).collect(Collectors.toList());
        Collections.sort(dtos, Comparator.comparing(ProcessGroupDTO::getName));

        dtos.stream().forEach(dto -> output.println(dto.getName() + " - " + dto.getId()));
    }

    @Override
    public void writeCurrentUser(CurrentUserEntity currentUserEntity, PrintStream output) {
        if (currentUserEntity == null) {
            return;
        }

        output.println(currentUserEntity.getIdentity());
    }

    @Override
    public void writeCurrentUser(CurrentUser currentUser, PrintStream output) {
        if (currentUser == null) {
            return;
        }

        output.println(currentUser.getIdentity());
    }
}
