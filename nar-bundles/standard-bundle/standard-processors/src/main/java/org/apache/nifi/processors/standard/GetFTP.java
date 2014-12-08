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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.FileTransfer;

@SideEffectFree
@Tags({"FTP", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches files from an FTP Server and creates FlowFiles from them")
public class GetFTP extends GetFileTransfer {

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FTPTransfer.HOSTNAME);
        properties.add(FTPTransfer.PORT);
        properties.add(FTPTransfer.USERNAME);
        properties.add(FTPTransfer.PASSWORD);
        properties.add(FTPTransfer.CONNECTION_MODE);
        properties.add(FTPTransfer.TRANSFER_MODE);
        properties.add(FTPTransfer.REMOTE_PATH);
        properties.add(FTPTransfer.FILE_FILTER_REGEX);
        properties.add(FTPTransfer.PATH_FILTER_REGEX);
        properties.add(FTPTransfer.POLLING_INTERVAL);
        properties.add(FTPTransfer.RECURSIVE_SEARCH);
        properties.add(FTPTransfer.IGNORE_DOTTED_FILES);
        properties.add(FTPTransfer.DELETE_ORIGINAL);
        properties.add(FTPTransfer.CONNECTION_TIMEOUT);
        properties.add(FTPTransfer.DATA_TIMEOUT);
        properties.add(FTPTransfer.MAX_SELECTS);
        properties.add(FTPTransfer.REMOTE_POLL_BATCH_SIZE);
        properties.add(FTPTransfer.USE_NATURAL_ORDERING);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new FTPTransfer(context, getLogger());
    }
}
