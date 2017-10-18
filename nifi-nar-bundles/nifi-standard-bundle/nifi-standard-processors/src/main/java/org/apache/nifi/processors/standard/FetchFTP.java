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

import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.FetchFileTransfer;
import org.apache.nifi.processors.standard.GetFTP;
import org.apache.nifi.processors.standard.GetSFTP;
import org.apache.nifi.processors.standard.PutFTP;
import org.apache.nifi.processors.standard.PutSFTP;
import org.apache.nifi.processors.standard.util.FTPTransfer;

// Note that we do not use @SupportsBatching annotation. This processor cannot support batching because it must ensure that session commits happen before remote files are deleted.
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"ftp", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches the content of a file from a remote SFTP server and overwrites the contents of an incoming FlowFile with the content of the remote file.")
@SeeAlso({GetSFTP.class, PutSFTP.class, GetFTP.class, PutFTP.class})
@WritesAttributes({
    @WritesAttribute(attribute = "ftp.remote.host", description = "The hostname or IP address from which the file was pulled"),
    @WritesAttribute(attribute = "ftp.remote.port", description = "The port that was used to communicate with the remote FTP server"),
    @WritesAttribute(attribute = "ftp.remote.filename", description = "The name of the remote file that was pulled"),
    @WritesAttribute(attribute = "filename", description = "The filename is updated to point to the filename fo the remote file"),
    @WritesAttribute(attribute = "path", description = "If the Remote File contains a directory name, that directory name will be added to the FlowFile using the 'path' attribute")
})
public class FetchFTP extends FetchFileTransfer {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor port = new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("21").build();

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOSTNAME);
        properties.add(port);
        properties.add(USERNAME);
        properties.add(FTPTransfer.PASSWORD);
        properties.add(REMOTE_FILENAME);
        properties.add(COMPLETION_STRATEGY);
        properties.add(MOVE_DESTINATION_DIR);
        properties.add(FTPTransfer.CONNECTION_TIMEOUT);
        properties.add(FTPTransfer.DATA_TIMEOUT);
        properties.add(FTPTransfer.USE_COMPRESSION);
        properties.add(FTPTransfer.CONNECTION_MODE);
        properties.add(FTPTransfer.TRANSFER_MODE);
        properties.add(FTPTransfer.PROXY_TYPE);
        properties.add(FTPTransfer.PROXY_HOST);
        properties.add(FTPTransfer.PROXY_PORT);
        properties.add(FTPTransfer.HTTP_PROXY_USERNAME);
        properties.add(FTPTransfer.HTTP_PROXY_PASSWORD);
        properties.add(FTPTransfer.BUFFER_SIZE);
        return properties;
    }

    @Override
    protected FileTransfer createFileTransfer(final ProcessContext context) {
        return new FTPTransfer(context, getLogger());
    }
}
