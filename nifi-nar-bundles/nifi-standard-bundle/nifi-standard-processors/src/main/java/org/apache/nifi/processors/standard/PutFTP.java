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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.FTPTransfer;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "copy", "egress", "put", "ftp", "archive", "files"})
@CapabilityDescription("Sends FlowFiles to an FTP Server")
@SeeAlso(GetFTP.class)
@DynamicProperties({
    @DynamicProperty(name = "pre.cmd._____", value = "Not used", description = "The command specified in the key will be executed before doing a put.  You may add these optional properties "
            + " to send any commands to the FTP server before the file is actually transferred (before the put command)."
            + " This option is only available for the PutFTP processor, as only FTP has this functionality. This is"
            + " essentially the same as sending quote commands to an FTP server from the command line.  While this is the same as sending a quote command, it is very important that"
            + " you leave off the ."),
    @DynamicProperty(name = "post.cmd._____", value = "Not used", description = "The command specified in the key will be executed after doing a put.  You may add these optional properties "
            + " to send any commands to the FTP server before the file is actually transferred (before the put command)."
            + " This option is only available for the PutFTP processor, as only FTP has this functionality. This is"
            + " essentially the same as sending quote commands to an FTP server from the command line.  While this is the same as sending a quote command, it is very important that"
            + " you leave off the .")})
public class PutFTP extends PutFileTransfer<FTPTransfer> {

    private static final Pattern PRE_SEND_CMD_PATTERN = Pattern.compile("^pre\\.cmd\\.(\\d+)$");
    private static final Pattern POST_SEND_CMD_PATTERN = Pattern.compile("^post\\.cmd\\.(\\d+)$");

    private final AtomicReference<List<PropertyDescriptor>> preSendDescriptorRef = new AtomicReference<>();
    private final AtomicReference<List<PropertyDescriptor>> postSendDescriptorRef = new AtomicReference<>();

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FTPTransfer.HOSTNAME);
        properties.add(FTPTransfer.PORT);
        properties.add(FTPTransfer.USERNAME);
        properties.add(FTPTransfer.PASSWORD);
        properties.add(FTPTransfer.REMOTE_PATH);
        properties.add(FTPTransfer.CREATE_DIRECTORY);
        properties.add(FTPTransfer.BATCH_SIZE);
        properties.add(FTPTransfer.CONNECTION_TIMEOUT);
        properties.add(FTPTransfer.DATA_TIMEOUT);
        properties.add(FTPTransfer.CONFLICT_RESOLUTION);
        properties.add(FTPTransfer.DOT_RENAME);
        properties.add(FTPTransfer.TEMP_FILENAME);
        properties.add(FTPTransfer.TRANSFER_MODE);
        properties.add(FTPTransfer.CONNECTION_MODE);
        properties.add(FTPTransfer.REJECT_ZERO_BYTE);
        properties.add(FTPTransfer.LAST_MODIFIED_TIME);
        properties.add(FTPTransfer.PERMISSIONS);
        properties.add(FTPTransfer.USE_COMPRESSION);
        properties.add(FTPTransfer.PROXY_TYPE);
        properties.add(FTPTransfer.PROXY_HOST);
        properties.add(FTPTransfer.PROXY_PORT);
        properties.add(FTPTransfer.HTTP_PROXY_USERNAME);
        properties.add(FTPTransfer.HTTP_PROXY_PASSWORD);
        properties.add(FTPTransfer.BUFFER_SIZE);
        properties.add(FTPTransfer.UTF8_ENCODING);

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void beforePut(final FlowFile flowFile, final ProcessContext context, final FTPTransfer transfer) throws IOException {
        transfer.sendCommands(getCommands(preSendDescriptorRef.get(), context, flowFile), flowFile);
    }

    @Override
    protected void afterPut(final FlowFile flowFile, final ProcessContext context, final FTPTransfer transfer) throws IOException {
        transfer.sendCommands(getCommands(postSendDescriptorRef.get(), context, flowFile), flowFile);
    }

    @Override
    protected FTPTransfer getFileTransfer(final ProcessContext context) {
        return new FTPTransfer(context, getLogger());
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void determinePrePostSendProperties(final ProcessContext context) {
        final Map<Integer, PropertyDescriptor> preDescriptors = new TreeMap<>();
        final Map<Integer, PropertyDescriptor> postDescriptors = new TreeMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            final String name = descriptor.getName();
            final Matcher preMatcher = PRE_SEND_CMD_PATTERN.matcher(name);
            if (preMatcher.matches()) {
                final int index = Integer.parseInt(preMatcher.group(1));
                preDescriptors.put(index, descriptor);
            } else {
                final Matcher postMatcher = POST_SEND_CMD_PATTERN.matcher(name);
                if (postMatcher.matches()) {
                    final int index = Integer.parseInt(postMatcher.group(1));
                    postDescriptors.put(index, descriptor);
                }
            }
        }

        final List<PropertyDescriptor> preDescriptorList = new ArrayList<>(preDescriptors.values());
        final List<PropertyDescriptor> postDescriptorList = new ArrayList<>(postDescriptors.values());
        this.preSendDescriptorRef.set(preDescriptorList);
        this.postSendDescriptorRef.set(postDescriptorList);
    }

    private List<String> getCommands(final List<PropertyDescriptor> descriptors, final ProcessContext context, final FlowFile flowFile) {
        final List<String> cmds = new ArrayList<>();
        for (final PropertyDescriptor descriptor : descriptors) {
            cmds.add(context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue());
        }

        return cmds;
    }
}
