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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
//import org.apache.nifi.processors.standard.util.SCPTransfer;
import org.apache.nifi.processors.standard.util.SCPTransfer;

@SupportsBatching
@Tags({"remote", "copy", "egress", "put", "scp", "archive", "files"})
@CapabilityDescription("Sends FlowFiles to an SCP Server")
@SeeAlso(GetSCP.class)
@DynamicProperty(name = "Disable Directory Listing", value = "true or false",
        description = "Disables directory listings before operations which might fail, such as configurations which create directory structures.")
public class PutSCP extends PutFileTransfer<SCPTransfer> {

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
        properties.add(SCPTransfer.CREATE_DIRECTORY);
        properties.add(SCPTransfer.BATCH_SIZE);
        properties.add(SCPTransfer.CONNECTION_TIMEOUT);
        properties.add(SCPTransfer.DATA_TIMEOUT);
        properties.add(SCPTransfer.CONFLICT_RESOLUTION);
        properties.add(SCPTransfer.REJECT_ZERO_BYTE);
        properties.add(SCPTransfer.DOT_RENAME);
        properties.add(SCPTransfer.TEMP_FILENAME);
        properties.add(SCPTransfer.HOST_KEY_FILE);
        properties.add(SCPTransfer.LAST_MODIFIED_TIME);
        properties.add(SCPTransfer.PERMISSIONS);
        properties.add(SCPTransfer.REMOTE_OWNER);
        properties.add(SCPTransfer.REMOTE_GROUP);
        properties.add(SCPTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SCPTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(SCPTransfer.USE_COMPRESSION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        if (SCPTransfer.DISABLE_DIRECTORY_LISTING.getName().equalsIgnoreCase(propertyDescriptorName)) {
            return SCPTransfer.DISABLE_DIRECTORY_LISTING;
        }
        return super.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @Override
    protected SCPTransfer getFileTransfer(final ProcessContext context) {
        return new SCPTransfer(context, getLogger());
    }

}
