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
import org.apache.nifi.annotation.behavior.SupportsBatching;
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
import org.apache.nifi.processors.standard.util.SCPTransfer;
import org.apache.nifi.processors.standard.util.SSHTransfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "copy", "egress", "put", "scp", "archive", "files"})
@CapabilityDescription("Sends FlowFiles to an SSH Server via SCP")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on the remote server"),
        @WritesAttribute(attribute = "absolute.path", description = "The full/absolute path where the file was uploaded to.")})
@SeeAlso(GetSCP.class)
public class PutSCP extends PutFileTransfer<SCPTransfer> {

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SSHTransfer.HOSTNAME);
        properties.add(SSHTransfer.PORT);
        properties.add(SSHTransfer.USERNAME);
        properties.add(SSHTransfer.PASSWORD);
        properties.add(SSHTransfer.PRIVATE_KEY_PATH);
        properties.add(SSHTransfer.PRIVATE_KEY_PASSPHRASE);
        properties.add(SSHTransfer.REMOTE_PATH);
        properties.add(SSHTransfer.CREATE_DIRECTORY);
        properties.add(SSHTransfer.BATCH_SIZE);
        properties.add(SSHTransfer.CONNECTION_TIMEOUT);
        properties.add(SSHTransfer.DATA_TIMEOUT);
        properties.add(SSHTransfer.CONFLICT_RESOLUTION);
        properties.add(SSHTransfer.REJECT_ZERO_BYTE);
        properties.add(SSHTransfer.DOT_RENAME);
        properties.add(SSHTransfer.TEMP_FILENAME);
        properties.add(SSHTransfer.HOST_KEY_FILE);
        properties.add(SSHTransfer.LAST_MODIFIED_TIME);
        properties.add(SSHTransfer.PERMISSIONS);
        properties.add(SSHTransfer.REMOTE_OWNER);
        properties.add(SSHTransfer.REMOTE_GROUP);
        properties.add(SSHTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SSHTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(SSHTransfer.USE_COMPRESSION);
        properties.add(SSHTransfer.PROXY_CONFIGURATION_SERVICE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected SCPTransfer getFileTransfer(final ProcessContext context) {
        return new SCPTransfer(context, getLogger());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        SSHTransfer.validateProxySpec(validationContext, results);
        return results;
    }
}
