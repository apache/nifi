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
package org.apache.nifi.controller;

import static java.util.Objects.requireNonNull;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.persistence.TemplateDeserializer;
import org.apache.nifi.persistence.TemplateSerializer;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.DataOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemplateManager {

    private static final Logger logger = LoggerFactory.getLogger(TemplateManager.class);

    private final Path directory;
    private final Map<String, Template> templateMap = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final FileFilter templateFileFilter = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.getName().toLowerCase().endsWith(".template");
        }
    };

    public TemplateManager(final Path storageLocation) throws IOException {
        directory = storageLocation;
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        } else {
            if (!Files.isDirectory(directory)) {
                throw new IllegalArgumentException(directory.toString() + " is not a directory");
            }
            // use toFile().canXXX, rather than Files.is... because on Windows 7, we sometimes get the wrong result for Files.is... (running Java 7 update 9)
            if (!directory.toFile().canExecute() || !directory.toFile().canWrite()) {
                throw new IOException("Invalid permissions for directory " + directory.toString());
            }
        }
    }

    /**
     * Adds a template to this manager. The contents of this template must be part of the current flow. This is going create a template based on a snippet of this flow. Any sensitive properties in the
     * TemplateDTO will be removed.
     *
     * @param dto dto
     * @return a copy of the given DTO
     * @throws IOException if an I/O error occurs when persisting the Template
     * @throws NullPointerException if the DTO is null
     * @throws IllegalArgumentException if does not contain all required information, such as the template name or a processor's configuration element
     */
    public Template addTemplate(final TemplateDTO dto) throws IOException {
        scrubTemplate(dto.getSnippet());
        return importTemplate(dto);
    }

    private void verifyCanImport(final TemplateDTO dto) {
        // ensure the template is specified
        if (dto == null || dto.getSnippet() == null) {
            throw new IllegalArgumentException("Template details not specified.");
        }

        // ensure the name is specified
        if (StringUtils.isBlank(dto.getName())) {
            throw new IllegalArgumentException("Template name cannot be blank.");
        }

        readLock.lock();
        try {
            for (final Template template : templateMap.values()) {
                final TemplateDTO existingDto = template.getDetails();

                // ensure a template with this name doesnt already exist
                if (dto.getName().equals(existingDto.getName())) {
                    throw new IllegalStateException(String.format("A template named '%s' already exists.", dto.getName()));
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Clears all Templates from the TemplateManager
     *
     * @throws java.io.IOException ioe
     */
    public void clear() throws IOException {
        writeLock.lock();
        try {
            templateMap.clear();

            final File[] files = directory.toFile().listFiles(templateFileFilter);
            if (files == null) {
                return;
            }

            for (final File file : files) {
                boolean successful = false;

                for (int i = 0; i < 10; i++) {
                    if (file.delete()) {
                        successful = true;
                        break;
                    }
                }

                if (!successful && file.exists()) {
                    throw new IOException("Failed to delete template file " + file.getAbsolutePath());
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @param id template id
     * @return the template with the given id, if it exists; else, returns null
     */
    public Template getTemplate(final String id) {
        readLock.lock();
        try {
            return templateMap.get(id);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Loads the templates from disk
     *
     * @throws IOException ioe
     */
    public void loadTemplates() throws IOException {
        writeLock.lock();
        try {
            final File[] files = directory.toFile().listFiles(templateFileFilter);
            if (files == null) {
                return;
            }

            for (final File file : files) {
                try (final FileInputStream fis = new FileInputStream(file);
                        final BufferedInputStream bis = new BufferedInputStream(fis)) {
                    final TemplateDTO templateDto = TemplateDeserializer.deserialize(bis);
                    templateMap.put(templateDto.getId(), new Template(templateDto));
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Template importTemplate(final TemplateDTO dto) throws IOException {
        // ensure we can add this template
        verifyCanImport(dto);

        writeLock.lock();
        try {
            if (requireNonNull(dto).getId() == null) {
                dto.setId(UUID.randomUUID().toString());
            }

            final Template template = new Template(dto);
            persistTemplate(template);

            templateMap.put(dto.getId(), template);
            return template;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Persists the given template to disk
     *
     * @param template template
     * @throws IOException ioe
     */
    private void persistTemplate(final Template template) throws IOException {
        final Path path = directory.resolve(template.getDetails().getId() + ".template");
        Files.write(path, TemplateSerializer.serialize(template.getDetails()), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    /**
     * Scrubs the template prior to persisting in order to remove fields that shouldn't be included or are unnecessary.
     *
     * @param snippet snippet
     */
    private void scrubTemplate(final FlowSnippetDTO snippet) {
        // ensure that contents have been specified
        if (snippet != null) {
            // go through each processor if specified
            if (snippet.getProcessors() != null) {
                scrubProcessors(snippet.getProcessors());
            }

            // go through each connection if specified
            if (snippet.getConnections() != null) {
                scrubConnections(snippet.getConnections());
            }

            // go through each remote process group if specified
            if (snippet.getRemoteProcessGroups() != null) {
                scrubRemoteProcessGroups(snippet.getRemoteProcessGroups());
            }

            // go through each process group if specified
            if (snippet.getProcessGroups() != null) {
                scrubProcessGroups(snippet.getProcessGroups());
            }

            // go through each controller service if specified
            if (snippet.getControllerServices() != null) {
                scrubControllerServices(snippet.getControllerServices());
            }
        }
    }

    /**
     * Scrubs process groups prior to saving.
     *
     * @param processGroups groups
     */
    private void scrubProcessGroups(final Set<ProcessGroupDTO> processGroups) {
        // go through each process group
        for (final ProcessGroupDTO processGroupDTO : processGroups) {
            scrubTemplate(processGroupDTO.getContents());
        }
    }

    /**
     * Scrubs processors prior to saving. This includes removing sensitive properties, validation errors, property descriptors, etc.
     *
     * @param processors procs
     */
    private void scrubProcessors(final Set<ProcessorDTO> processors) {
        // go through each processor
        for (final ProcessorDTO processorDTO : processors) {
            final ProcessorConfigDTO processorConfig = processorDTO.getConfig();

            // ensure that some property configuration have been specified
            if (processorConfig != null) {
                // if properties have been specified, remove sensitive ones
                if (processorConfig.getProperties() != null) {
                    Map<String, String> processorProperties = processorConfig.getProperties();

                    // look for sensitive properties and remove them
                    if (processorConfig.getDescriptors() != null) {
                        final Collection<PropertyDescriptorDTO> descriptors = processorConfig.getDescriptors().values();
                        for (PropertyDescriptorDTO descriptor : descriptors) {
                            if (descriptor.isSensitive()) {
                                processorProperties.put(descriptor.getName(), null);
                            }
                        }
                    }
                }

                processorConfig.setCustomUiUrl(null);
            }

            // remove validation errors
            processorDTO.setValidationErrors(null);
        }
    }

    private void scrubControllerServices(final Set<ControllerServiceDTO> controllerServices) {
        for (final ControllerServiceDTO serviceDTO : controllerServices) {
            final Map<String, String> properties = serviceDTO.getProperties();
            final Map<String, PropertyDescriptorDTO> descriptors = serviceDTO.getDescriptors();

            if (properties != null && descriptors != null) {
                for (final PropertyDescriptorDTO descriptor : descriptors.values()) {
                    if (descriptor.isSensitive()) {
                        properties.put(descriptor.getName(), null);
                    }
                }
            }

            serviceDTO.setCustomUiUrl(null);
            serviceDTO.setValidationErrors(null);
        }
    }

    /**
     * Scrubs connections prior to saving. This includes removing available relationships.
     *
     * @param connections conns
     */
    private void scrubConnections(final Set<ConnectionDTO> connections) {
        // go through each connection
        for (final ConnectionDTO connectionDTO : connections) {
            connectionDTO.setAvailableRelationships(null);

            scrubConnectable(connectionDTO.getSource());
            scrubConnectable(connectionDTO.getDestination());
        }
    }

    /**
     * Remove unnecessary fields in connectables prior to saving.
     *
     * @param connectable connectable
     */
    private void scrubConnectable(final ConnectableDTO connectable) {
        if (connectable != null) {
            connectable.setComments(null);
            connectable.setExists(null);
            connectable.setRunning(null);
            connectable.setTransmitting(null);
            connectable.setName(null);
        }
    }

    /**
     * Remove unnecessary fields in remote groups prior to saving.
     *
     * @param remoteGroups groups
     */
    private void scrubRemoteProcessGroups(final Set<RemoteProcessGroupDTO> remoteGroups) {
        // go through each remote process group
        for (final RemoteProcessGroupDTO remoteProcessGroupDTO : remoteGroups) {
            remoteProcessGroupDTO.setFlowRefreshed(null);
            remoteProcessGroupDTO.setInputPortCount(null);
            remoteProcessGroupDTO.setOutputPortCount(null);
            remoteProcessGroupDTO.setTransmitting(null);

            // if this remote process group has contents
            if (remoteProcessGroupDTO.getContents() != null) {
                RemoteProcessGroupContentsDTO contents = remoteProcessGroupDTO.getContents();

                // scrub any remote input ports
                if (contents.getInputPorts() != null) {
                    scrubRemotePorts(contents.getInputPorts());
                }

                // scrub and remote output ports
                if (contents.getOutputPorts() != null) {
                    scrubRemotePorts(contents.getOutputPorts());
                }
            }
        }
    }

    /**
     * Remove unnecessary fields in remote ports prior to saving.
     *
     * @param remotePorts ports
     */
    private void scrubRemotePorts(final Set<RemoteProcessGroupPortDTO> remotePorts) {
        for (final Iterator<RemoteProcessGroupPortDTO> remotePortIter = remotePorts.iterator(); remotePortIter.hasNext();) {
            final RemoteProcessGroupPortDTO remotePortDTO = remotePortIter.next();

            // if the flow is not connected to this remote port, remove it
            if (remotePortDTO.isConnected() == null || !remotePortDTO.isConnected().booleanValue()) {
                remotePortIter.remove();
                continue;
            }

            remotePortDTO.setExists(null);
            remotePortDTO.setTargetRunning(null);
        }
    }

    /**
     * Removes the template with the given ID.
     *
     * @param id the ID of the template to remove
     * @throws NullPointerException if the argument is null
     * @throws IllegalStateException if no template exists with the given ID
     * @throws IOException if template could not be removed
     */
    public void removeTemplate(final String id) throws IOException, IllegalStateException {
        writeLock.lock();
        try {
            final Template removed = templateMap.remove(requireNonNull(id));

            // ensure the template exists
            if (removed == null) {
                throw new IllegalStateException("No template with ID " + id + " exists");
            } else {

                try {
                    // remove the template from the archive directory
                    final Path path = directory.resolve(removed.getDetails().getId() + ".template");
                    Files.delete(path);
                } catch (final NoSuchFileException e) {
                    logger.warn(String.format("Template file for template %s not found when attempting to remove. Continuing...", id));
                } catch (final IOException e) {
                    logger.error(String.format("Unable to remove template file for template %s.", id));

                    // since the template file existed and we were unable to remove it, rollback
                    // by returning it to the template map
                    templateMap.put(id, removed);

                    // rethrow
                    throw e;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Set<Template> getTemplates() {
        readLock.lock();
        try {
            return new HashSet<>(templateMap.values());
        } finally {
            readLock.unlock();
        }
    }

    public static List<Template> parseBytes(final byte[] bytes) {
        final List<Template> templates = new ArrayList<>();

        try (final InputStream rawIn = new ByteArrayInputStream(bytes);
                final DataInputStream in = new DataInputStream(rawIn)) {

            while (isMoreData(in)) {
                final int length = in.readInt();
                final byte[] buffer = new byte[length];
                StreamUtils.fillBuffer(in, buffer, true);
                final TemplateDTO dto = TemplateDeserializer.deserialize(new ByteArrayInputStream(buffer));
                templates.add(new Template(dto));
            }
        } catch (final IOException e) {
            throw new RuntimeException("Could not parse bytes", e);  // won't happen because of the types of streams being used
        }

        return templates;
    }

    private static boolean isMoreData(final InputStream in) throws IOException {
        in.mark(1);
        final int nextByte = in.read();
        if (nextByte == -1) {
            return false;
        }

        in.reset();
        return true;
    }

    public byte[] export() {
        readLock.lock();
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream dos = new DataOutputStream(baos)) {
            for (final Template template : templateMap.values()) {
                final TemplateDTO dto = template.getDetails();
                final byte[] bytes = TemplateSerializer.serialize(dto);
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }

            return baos.toByteArray();
        } catch (final IOException e) {
            // won't happen
            return null;
        } finally {
            readLock.unlock();
        }
    }
}
