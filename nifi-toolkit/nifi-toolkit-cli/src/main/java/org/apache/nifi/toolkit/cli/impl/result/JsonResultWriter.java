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
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

/**
 * ResultWriter implementation that uses Jackson to serialize to JSON.
 */
public class JsonResultWriter implements ResultWriter {

    @Override
    public void writeBuckets(List<Bucket> buckets, PrintStream output) throws IOException {
        write(buckets, output);
    }

    @Override
    public void writeBucket(Bucket bucket, PrintStream output) throws IOException {
        write(bucket, output);
    }

    @Override
    public void writeFlows(List<VersionedFlow> versionedFlows, PrintStream output) throws IOException {
        write(versionedFlows, output);
    }

    @Override
    public void writeFlow(VersionedFlow versionedFlow, PrintStream output) throws IOException {
        write(versionedFlow, output);
    }

    @Override
    public void writeSnapshotMetadata(List<VersionedFlowSnapshotMetadata> versions, PrintStream output) throws IOException {
        write(versions, output);
    }

    @Override
    public void writeSnapshotMetadata(VersionedFlowSnapshotMetadata version, PrintStream output) throws IOException {
        write(version, output);
    }

    @Override
    public void writeRegistryClients(RegistryClientsEntity clientsEntity, PrintStream output) throws IOException {
        write(clientsEntity, output);
    }

    @Override
    public void writeVariables(VariableRegistryEntity variableRegistryEntity, PrintStream output) throws IOException {
        write(variableRegistryEntity, output);
    }

    @Override
    public void writeSnapshotMetadata(VersionedFlowSnapshotMetadataSetEntity versionedFlowSnapshotMetadataSetEntity, PrintStream output) throws IOException {
        write(versionedFlowSnapshotMetadataSetEntity, output);
    }

    @Override
    public void writeVersionControlInfo(VersionControlInformationEntity versionControlInformationEntity, PrintStream output) throws IOException {
        write(versionControlInformationEntity, output);
    }

    @Override
    public void writeProcessGroups(List<ProcessGroupEntity> processGroupEntities, PrintStream output) throws IOException {
        write(processGroupEntities, output);
    }

    @Override
    public void writeCurrentUser(CurrentUserEntity currentUserEntity, PrintStream output) throws IOException {
        write(currentUserEntity, output);
    }

    @Override
    public void writeCurrentUser(CurrentUser currentUser, PrintStream output) throws IOException {
        write(currentUser, output);
    }

    private void write(final Object result, final OutputStream output) throws IOException {
        JacksonUtils.write(result, output);
    }

}
