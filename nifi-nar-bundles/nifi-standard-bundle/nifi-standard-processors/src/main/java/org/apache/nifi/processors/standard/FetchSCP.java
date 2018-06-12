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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SCPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTransfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// Note that we do not use @SupportsBatching annotation. This processor cannot support batching because it must ensure that session commits happen before remote files are deleted.
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"scp", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches the content of a file from a remote SCP server and overwrites the contents of an incoming FlowFile with the content of the remote file.")
@SeeAlso({GetSCP.class, PutSCP.class, ListRemoteFiles.class})
@WritesAttributes({
    @WritesAttribute(attribute = "ssh.remote.host", description = "The hostname or IP address from which the file was pulled"),
    @WritesAttribute(attribute = "ssh.remote.port", description = "The port that was used to communicate with the remote SFTP server"),
    @WritesAttribute(attribute = "ssh.remote.filename", description = "The name of the remote file that was pulled"),
    @WritesAttribute(attribute = "filename", description = "The filename is updated to point to the filename fo the remote file"),
    @WritesAttribute(attribute = "path", description = "If the Remote File contains a directory name, that directory name will be added to the FlowFile using the 'path' attribute")
})
public class FetchSCP extends FetchFileTransfer {

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
        properties.add(REMOTE_FILENAME);
        properties.add(COMPLETION_STRATEGY);
        properties.add(MOVE_DESTINATION_DIR);
        properties.add(MOVE_CREATE_DIRECTORY);
        properties.add(SSHTransfer.CONNECTION_TIMEOUT);
        properties.add(SSHTransfer.DATA_TIMEOUT);
        properties.add(SSHTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(SSHTransfer.HOST_KEY_FILE);
        properties.add(SSHTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SSHTransfer.USE_COMPRESSION);
        properties.add(SSHTransfer.PROXY_CONFIGURATION_SERVICE);
        return properties;
    }

    @Override
    protected FileTransfer createFileTransfer(final ProcessContext context) {
        return new SCPTransfer(context, getLogger());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        SSHTransfer.validateProxySpec(validationContext, results);
        return results;
    }
}
