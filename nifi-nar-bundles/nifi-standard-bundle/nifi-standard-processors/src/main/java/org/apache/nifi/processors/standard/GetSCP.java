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

// adding scp eorgad 2016-05-04

package org.apache.nifi.processors.standard;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.standard.util.FileTransfer;
//import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SCPTransfer;

@Tags({"scp", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches files from an SCP Server and creates FlowFiles from them")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on the remote server"),
    @WritesAttribute(attribute = "path", description = "The path is set to the path of the file's directory on the remote server. "
            + "For example, if the <Remote Path> property is set to /tmp, files picked up from /tmp will have the path attribute set "
            + "to /tmp. If the <Search Recursively> property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path "
            + "attribute will be set to /tmp/abc/1/2/3"),
    @WritesAttribute(attribute = "file.lastModifiedTime", description = "The date and time that the source file was last modified"),
    @WritesAttribute(attribute = "file.owner", description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = "file.group", description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = "file.permissions", description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = "absolute.path", description = "The full/absolute path from where a file was picked up. The current 'path' "
            + "attribute is still populated, but may be a relative path")})
@SeeAlso(PutSCP.class)
public class GetSCP extends GetFileTransfer {

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCPTransfer.HOSTNAME);
        properties.add(SCPTransfer.PORT);
        properties.add(SCPTransfer.USERNAME);
        properties.add(SCPTransfer.PASSWORD);
        properties.add(SCPTransfer.PRIVATE_KEY_PATH);
        properties.add(SCPTransfer.PRIVATE_KEY_PASSPHRASE);
        properties.add(SCPTransfer.REMOTE_PATH);
        properties.add(SCPTransfer.FILE_FILTER_REGEX);
        properties.add(SCPTransfer.PATH_FILTER_REGEX);
        properties.add(SCPTransfer.POLLING_INTERVAL);
        properties.add(SCPTransfer.RECURSIVE_SEARCH);
        properties.add(SCPTransfer.IGNORE_DOTTED_FILES);
        properties.add(SCPTransfer.DELETE_ORIGINAL);
        properties.add(SCPTransfer.CONNECTION_TIMEOUT);
        properties.add(SCPTransfer.DATA_TIMEOUT);
        properties.add(SCPTransfer.HOST_KEY_FILE);
        properties.add(SCPTransfer.MAX_SELECTS);
        properties.add(SCPTransfer.REMOTE_POLL_BATCH_SIZE);
        properties.add(SCPTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SCPTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(SCPTransfer.USE_COMPRESSION);
        properties.add(SCPTransfer.USE_NATURAL_ORDERING);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));
        final boolean passwordSpecified = context.getProperty(SCPTransfer.PASSWORD).getValue() != null;
        final boolean privateKeySpecified = context.getProperty(SCPTransfer.PRIVATE_KEY_PATH).getValue() != null;

        if (!passwordSpecified && !privateKeySpecified) {
            results.add(new ValidationResult.Builder().subject("Password")
                    .explanation("Either the Private Key Passphrase or the Password must be supplied")
                    .valid(false)
                    .build());
        }

        return results;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
    	return new SCPTransfer(context, getLogger());
    }
}
