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
package org.apache.nifi.processors.transfer;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.InputStream;
import java.util.Optional;

import static org.apache.nifi.processors.transfer.ResourceTransferProperties.FILE_RESOURCE_SERVICE;

public final class ResourceTransferUtils {

    private ResourceTransferUtils() {}

    /**
     * Get File Resource from File Resource Service based on provided Source otherwise return empty
     *
     * @param resourceTransferSource type of the data upload
     * @param context process context with properties
     * @param flowFile FlowFile with attributes to use in expression language
     * @return Optional FileResource retrieved from FileResourceService if Source is File Resource Service, otherwise empty
     * @throws ProcessException Thrown if Source is File Resource but FileResourceService is not provided in the context
     */
    public static Optional<FileResource> getFileResource(final ResourceTransferSource resourceTransferSource, final ProcessContext context, final FlowFile flowFile) {
        final Optional<FileResource> resource;

        if (resourceTransferSource == ResourceTransferSource.FILE_RESOURCE_SERVICE) {
            final PropertyValue property = context.getProperty(FILE_RESOURCE_SERVICE);
            if (property == null || !property.isSet()) {
                throw new ProcessException("File Resource Service required but not configured");
            }
            final FileResourceService fileResourceService = property.asControllerService(FileResourceService.class);
            final FileResource fileResource = fileResourceService.getFileResource(flowFile.getAttributes());
            resource = Optional.ofNullable(fileResource);
        } else {
            resource = Optional.empty();
        }

        return resource;
    }

    /**
     * Returns the input stream of the FileResource if it is provided (not null). Otherwise, returns the input stream of the FlowFile.
     *
     * @param session the session to read the FlowFile
     * @param flowFile the FlowFile which is read when no FileResource is provided
     * @param fileResource the FileResource
     * @return input stream of the FileResource or the FlowFile
     */
    public static InputStream getTransferInputStream(final ProcessSession session, final FlowFile flowFile, final FileResource fileResource) {
        return fileResource == null ? session.read(flowFile) : fileResource.getInputStream();
    }

    /**
     * Returns the size of the FileResource if it is provided (not null). Otherwise, returns the size of the FlowFile.
     *
     * @param flowFile the FlowFile which is used when no FileResource is provided
     * @param fileResource the FileResource
     * @return size of the FileResource or the FlowFile in bytes
     */
    public static long getTransferSize(final FlowFile flowFile, final FileResource fileResource) {
        return fileResource == null ? flowFile.getSize() : fileResource.getSize();
    }
}
