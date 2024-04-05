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

package org.apache.nifi.processor.util.file.transfer;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class ListFileTransfer extends AbstractListProcessor<FileInfo> {
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
        .name("Hostname")
        .description("The fully qualified hostname or IP address of the remote system")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();
    public static final PropertyDescriptor UNDEFAULTED_PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .description("The port to connect to on the remote host to fetch the data from")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(true)
        .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("Username")
        .description("Username")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(true)
        .build();
    public static final PropertyDescriptor REMOTE_PATH = new PropertyDescriptor.Builder()
        .name("Remote Path")
        .description("The path on the remote system from which to pull or push files")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .defaultValue(".")
        .build();
    public static final PropertyDescriptor FILE_TRANSFER_LISTING_STRATEGY = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(LISTING_STRATEGY)
        .allowableValues(BY_TIMESTAMPS, BY_ENTITIES, NO_TRACKING, BY_TIME_WINDOW)
        .build();

    @Override
    protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(GetFileTransfer.FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
        attributes.put(getProtocolName() + ".remote.host", context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue());
        attributes.put(getProtocolName() + ".remote.port", context.getProperty(UNDEFAULTED_PORT).evaluateAttributeExpressions().getValue());
        attributes.put(getProtocolName() + ".listing.user", context.getProperty(USERNAME).evaluateAttributeExpressions().getValue());
        attributes.put(GetFileTransfer.FILE_LAST_MODIFY_TIME_ATTRIBUTE, dateTimeFormatter.format(Instant.ofEpochMilli(fileInfo.getLastModifiedTime()).atZone(ZoneId.systemDefault())));
        attributes.put(GetFileTransfer.FILE_PERMISSIONS_ATTRIBUTE, fileInfo.getPermissions());
        attributes.put(GetFileTransfer.FILE_OWNER_ATTRIBUTE, fileInfo.getOwner());
        attributes.put(GetFileTransfer.FILE_GROUP_ATTRIBUTE, fileInfo.getGroup());
        attributes.put(GetFileTransfer.FILE_SIZE_ATTRIBUTE, Long.toString(fileInfo.getSize()));
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getFileName());
        final String fullPath = fileInfo.getFullPathFileName();
        if (fullPath != null) {
            final int index = fullPath.lastIndexOf("/");
            if (index > -1) {
                final String path = fullPath.substring(0, index);
                attributes.put(CoreAttributes.PATH.key(), path);
            }
        }
        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(REMOTE_PATH).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, 0L, ListingMode.CONFIGURATION_VERIFICATION, false).size();
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode) throws IOException {
        return performListing(context, minTimestamp, listingMode, true);
    }

    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                            final boolean applyFilters) throws IOException {
        final FileTransfer transfer = getFileTransfer(context);
        final List<FileInfo> listing;
        try {
            listing = transfer.getListing(applyFilters);
        } finally {
            IOUtils.closeQuietly(transfer);
        }

        if (minTimestamp == null) {
            return listing;
        }

        listing.removeIf(file -> file.getLastModifiedTime() < minTimestamp);

        return listing;
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("Remote Directory [%s] on [%s:%s]", getPath(context), context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(UNDEFAULTED_PORT).evaluateAttributeExpressions().getValue());
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return FileInfo.getRecordSchema();
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return HOSTNAME.equals(property) || REMOTE_PATH.equals(property);
    }

    protected abstract FileTransfer getFileTransfer(final ProcessContext context);

    protected abstract String getProtocolName();
}
