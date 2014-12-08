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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

@SideEffectFree
@Tags({"sftp", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches files from an SFTP Server and creates FlowFiles from them")
public class GetSFTP extends GetFileTransfer {

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
        properties.add(SFTPTransfer.FILE_FILTER_REGEX);
        properties.add(SFTPTransfer.PATH_FILTER_REGEX);
        properties.add(SFTPTransfer.POLLING_INTERVAL);
        properties.add(SFTPTransfer.RECURSIVE_SEARCH);
        properties.add(SFTPTransfer.IGNORE_DOTTED_FILES);
        properties.add(SFTPTransfer.DELETE_ORIGINAL);
        properties.add(SFTPTransfer.CONNECTION_TIMEOUT);
        properties.add(SFTPTransfer.DATA_TIMEOUT);
        properties.add(SFTPTransfer.HOST_KEY_FILE);
        properties.add(SFTPTransfer.MAX_SELECTS);
        properties.add(SFTPTransfer.REMOTE_POLL_BATCH_SIZE);
        properties.add(SFTPTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(SFTPTransfer.USE_COMPRESSION);
        properties.add(SFTPTransfer.USE_NATURAL_ORDERING);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));
        final boolean passwordSpecified = context.getProperty(SFTPTransfer.PASSWORD).getValue() != null;
        final boolean privateKeySpecified = context.getProperty(SFTPTransfer.PRIVATE_KEY_PATH).getValue() != null;

        if (!passwordSpecified && !privateKeySpecified) {
            results.add(new ValidationResult.Builder().subject("Password").explanation("Either the Private Key Passphrase or the Password must be supplied").valid(false).build());
        }

        return results;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new SFTPTransfer(context, getLogger());
    }
}
