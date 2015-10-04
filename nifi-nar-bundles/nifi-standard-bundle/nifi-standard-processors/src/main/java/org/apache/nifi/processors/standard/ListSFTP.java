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

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

@TriggerSerially
@Tags({"list", "sftp", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("Performs a listing of the files residing on an SFTP server. For each file that is found on the remote server, a new FlowFile will be created with the filename attribute "
    + "set to the name of the file on the remote server. This can then be used in conjunction with FetchSFTP in order to fetch those files.")
@SeeAlso({FetchSFTP.class, GetSFTP.class, PutSFTP.class})
@WritesAttributes({
    @WritesAttribute(attribute = "sftp.remote.host", description = "The hostname of the SFTP Server"),
    @WritesAttribute(attribute = "file.owner", description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = "file.group", description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = "file.permissions", description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the SFTP Server"),
    @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the SFTP Server from which the file was pulled"),
})
public class ListSFTP extends ListFileTransfer {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SFTPTransfer.HOSTNAME);
        properties.add(SFTPTransfer.PORT);
        properties.add(SFTPTransfer.USERNAME);
        properties.add(SFTPTransfer.PASSWORD);
        properties.add(SFTPTransfer.PRIVATE_KEY_PATH);
        properties.add(SFTPTransfer.PRIVATE_KEY_PASSPHRASE);
        properties.add(REMOTE_PATH);
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(SFTPTransfer.RECURSIVE_SEARCH);
        properties.add(SFTPTransfer.FILE_FILTER_REGEX);
        properties.add(SFTPTransfer.PATH_FILTER_REGEX);
        properties.add(SFTPTransfer.IGNORE_DOTTED_FILES);
        properties.add(SFTPTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SFTPTransfer.HOST_KEY_FILE);
        properties.add(SFTPTransfer.CONNECTION_TIMEOUT);
        properties.add(SFTPTransfer.DATA_TIMEOUT);
        properties.add(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        return properties;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new SFTPTransfer(context, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "sftp";
    }
}
