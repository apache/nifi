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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "copy", "egress", "put", "sftp", "archive", "files"})
@CapabilityDescription("Sends FlowFiles to an SFTP Server")
@SeeAlso(GetSFTP.class)
@DynamicProperty(name = "Disable Directory Listing", value = "true or false",
        description = "Disables directory listings before operations which might fail, such as configurations which create directory structures.")
public class PutSFTP extends PutFileTransfer<SFTPTransfer> {

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SFTPTransfer.HOSTNAME);
        properties.add(SFTPTransfer.PORT);
        properties.add(SFTPTransfer.USERNAME);
        properties.add(SFTPTransfer.PASSWORD);
        properties.add(SFTPTransfer.PRIVATE_KEY_PATH);
        properties.add(SFTPTransfer.PRIVATE_KEY_PASSPHRASE);
        properties.add(SFTPTransfer.REMOTE_PATH);
        properties.add(SFTPTransfer.CREATE_DIRECTORY);
        properties.add(SFTPTransfer.DISABLE_DIRECTORY_LISTING);
        properties.add(SFTPTransfer.BATCH_SIZE);
        properties.add(SFTPTransfer.CONNECTION_TIMEOUT);
        properties.add(SFTPTransfer.DATA_TIMEOUT);
        properties.add(SFTPTransfer.CONFLICT_RESOLUTION);
        properties.add(SFTPTransfer.REJECT_ZERO_BYTE);
        properties.add(SFTPTransfer.DOT_RENAME);
        properties.add(SFTPTransfer.TEMP_FILENAME);
        properties.add(SFTPTransfer.HOST_KEY_FILE);
        properties.add(SFTPTransfer.LAST_MODIFIED_TIME);
        properties.add(SFTPTransfer.PERMISSIONS);
        properties.add(SFTPTransfer.REMOTE_OWNER);
        properties.add(SFTPTransfer.REMOTE_GROUP);
        properties.add(SFTPTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(SFTPTransfer.USE_COMPRESSION);
        properties.add(SFTPTransfer.PROXY_CONFIGURATION_SERVICE);
        properties.add(FTPTransfer.PROXY_TYPE);
        properties.add(FTPTransfer.PROXY_HOST);
        properties.add(FTPTransfer.PROXY_PORT);
        properties.add(FTPTransfer.HTTP_PROXY_USERNAME);
        properties.add(FTPTransfer.HTTP_PROXY_PASSWORD);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected SFTPTransfer getFileTransfer(final ProcessContext context) {
        return new SFTPTransfer(context, getLogger());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        SFTPTransfer.validateProxySpec(validationContext, results);
        return results;
    }
}
