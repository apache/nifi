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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.FileInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ListFile extends AbstractListProcessor<FileInfo> {
    public static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
        .name("Path")
        .description("The path on the system from which to pull or push files")
        .required(false)
        .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
        .expressionLanguageSupported(true)
        .defaultValue(".")
        .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PATH);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @Override
    protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("file.owner", fileInfo.getOwner());
        attributes.put("file.group", fileInfo.getGroup());
        attributes.put("file.permissions", fileInfo.getPermissions());
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
        return context.getProperty(PATH).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
        final File path = new File(getPath(context));
        final List<FileInfo> listing = new ArrayList<>();
        File[] files = path.listFiles();
        if (files != null) {
            for (File file : files) {
                final PosixFileAttributes attrib = Files.readAttributes(file.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                listing.add(new FileInfo.Builder()
                        .directory(file.isDirectory())
                        .filename(file.getName())
                        .fullPathFileName(file.getAbsolutePath())
                        .group(attrib.group().getName())
                        .lastModifiedTime(file.lastModified())
                        .owner(attrib.owner().getName())
                        .permissions(attrib.permissions().toString())
                        .size(file.getTotalSpace())
                        .build());
            }
        }
        if (minTimestamp == null) {
            return listing;
        }

        final Iterator<FileInfo> itr = listing.iterator();
        while (itr.hasNext()) {
            final FileInfo next = itr.next();
            if (next.getLastModifiedTime() < minTimestamp) {
                itr.remove();
            }
        }

        return listing;
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return PATH.equals(property);
    }
}
