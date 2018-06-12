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

package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SCPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTransfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"list", "ssh", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("Performs a listing of the files residing on a server using SSH. For each file that is found on the remote server, a new FlowFile will be created with the filename attribute "
    + "set to the name of the file on the remote server. This can then be used in conjunction with FetchSCP in order to fetch those files.")
@SeeAlso({FetchSCP.class, GetSCP.class, PutSCP.class})
@WritesAttributes({
    @WritesAttribute(attribute = "ssh.remote.host", description = "The hostname of the SSH Server"),
    @WritesAttribute(attribute = "ssh.remote.port", description = "The port that was connected to on the SSH Server"),
    @WritesAttribute(attribute = "ssh.listing.user", description = "The username of the user that performed the Listing"),
    @WritesAttribute(attribute = ListFile.FILE_OWNER_ATTRIBUTE, description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_GROUP_ATTRIBUTE, description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_PERMISSIONS_ATTRIBUTE, description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_SIZE_ATTRIBUTE, description = "The number of bytes in the source file"),
    @WritesAttribute(attribute = ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description = "The timestamp of when the file in the filesystem was" +
                  "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the Server"),
    @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the Server from which the file was pulled"),
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
    + "This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
    + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
public class ListRemoteFiles extends ListFileTransfer {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor port = new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("22").build();

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOSTNAME);
        properties.add(port);
        properties.add(USERNAME);
        properties.add(SSHTransfer.PASSWORD);
        properties.add(SSHTransfer.PRIVATE_KEY_PATH);
        properties.add(SSHTransfer.PRIVATE_KEY_PASSPHRASE);
        properties.add(REMOTE_PATH);
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(SSHTransfer.RECURSIVE_SEARCH);
        properties.add(SSHTransfer.FILE_FILTER_REGEX);
        properties.add(SSHTransfer.PATH_FILTER_REGEX);
        properties.add(SSHTransfer.IGNORE_DOTTED_FILES);
        properties.add(SSHTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SSHTransfer.HOST_KEY_FILE);
        properties.add(SSHTransfer.CONNECTION_TIMEOUT);
        properties.add(SSHTransfer.DATA_TIMEOUT);
        properties.add(SSHTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(TARGET_SYSTEM_TIMESTAMP_PRECISION);
        properties.add(SSHTransfer.PROXY_CONFIGURATION_SERVICE);
        return properties;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new SCPTransfer(context, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "ssh";
    }

    @Override
    protected Scope getStateScope(final ProcessContext context) {
        // Use cluster scope so that component can be run on Primary Node Only and can still
        // pick up where it left off, even if the Primary Node changes.
        return Scope.CLUSTER;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        SSHTransfer.validateProxySpec(validationContext, results);
        return results;
    }
}
