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
package org.apache.nifi.cluster.flow.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.nifi.cluster.flow.ClusterDataFlow;
import org.apache.nifi.cluster.flow.DaoException;
import org.apache.nifi.cluster.flow.DataFlowDao;
import org.apache.nifi.cluster.flow.PersistedFlowState;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.jaxb.message.NodeIdentifierAdapter;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Implements the FlowDao interface. The implementation tracks the state of the
 * dataflow by annotating the filename of the flow state file. Specifically, the
 * implementation correlates PersistedFlowState states to filename extensions.
 * The correlation is as follows:
 * <ul>
 * <li> CURRENT maps to flow.xml </li>
 * <li> STALE maps to flow.xml.stale </li>
 * <li> UNKNOWN maps to flow.xml.unknown </li>
 * </ul>
 * Whenever the flow state changes, the flow state file's name is updated to
 * denote its state.
 *
 * The implementation also provides for a restore directory that may be
 * configured for higher availability. At instance creation, if the primary or
 * restore directories have multiple flow state files, an exception is thrown.
 * If the primary directory has a current flow state file, but the restore
 * directory does not, then the primary flow state file is copied to the restore
 * directory. If the restore directory has a current flow state file, but the
 * primary directory does not, then the restore flow state file is copied to the
 * primary directory. If both the primary and restore directories have a current
 * flow state file and the files are different, then an exception is thrown.
 *
 * When the flow state file is saved, it is always saved first to the restore
 * directory followed by a save to the primary directory. When the flow state
 * file is loaded, a check is made to verify that the primary and restore flow
 * state files are both current. If either is not current, then an exception is
 * thrown. The primary flow state file is always read when the load method is
 * called.
 *
 * @author unattributed
 */
public class DataFlowDaoImpl implements DataFlowDao {

    private final File primaryDirectory;
    private final File restoreDirectory;
    private final boolean autoStart;
    private final String generatedRootGroupId = UUID.randomUUID().toString();

    public static final String STALE_EXT = ".stale";
    public static final String UNKNOWN_EXT = ".unknown";
    public static final String FLOW_PACKAGE = "flow.tar";
    public static final String FLOW_XML_FILENAME = "flow.xml";
    public static final String TEMPLATES_FILENAME = "templates.xml";
    public static final String SNIPPETS_FILENAME = "snippets.xml";
    public static final String CLUSTER_INFO_FILENAME = "cluster-info.xml";

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(DataFlowDaoImpl.class));

    public DataFlowDaoImpl(final File primaryDirectory) throws DaoException {
        this(primaryDirectory, null, false);
    }

    public DataFlowDaoImpl(final File primaryDirectory, final File restoreDirectory, final boolean autoStart) throws DaoException {

        // sanity check that primary directory is a directory, creating it if necessary
        if (primaryDirectory == null) {
            throw new IllegalArgumentException("Primary directory may not be null.");
        } else if (!primaryDirectory.exists()) {
            if (!primaryDirectory.mkdir()) {
                throw new DaoException(String.format("Failed to create primary directory '%s'", primaryDirectory.getAbsolutePath()));
            }
        } else if (!primaryDirectory.isDirectory()) {
            throw new IllegalArgumentException("Primary directory must be a directory.");
        }

        this.autoStart = autoStart;

        try {
            this.primaryDirectory = primaryDirectory;
            this.restoreDirectory = restoreDirectory;

            if (restoreDirectory == null) {
                // check that we have exactly one current flow state file
                ensureSingleCurrentStateFile(primaryDirectory);
            } else {

                // check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the primary directory
                if (primaryDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                    throw new IllegalArgumentException(String.format("Primary directory '%s' is the same as restore directory '%s' ",
                            primaryDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                }

                final File[] primaryFlowStateFiles = getFlowStateFiles(primaryDirectory);
                final File[] restoreFlowStateFiles = getFlowStateFiles(restoreDirectory);

                // if more than one state file in either primary or restore, then throw exception
                if (primaryFlowStateFiles.length > 1) {
                    throw new IllegalStateException(String.format("Found multiple dataflow state files in primary directory '%s'", primaryDirectory));
                } else if (restoreFlowStateFiles.length > 1) {
                    throw new IllegalStateException(String.format("Found multiple dataflow state files in restore directory '%s'", restoreDirectory));
                }

                // check that the single primary state file we found is current or create a new one
                final File primaryFlowStateFile = ensureSingleCurrentStateFile(primaryDirectory);

                // check that the single restore state file we found is current or create a new one
                final File restoreFlowStateFile = ensureSingleCurrentStateFile(restoreDirectory);

                // if there was a difference in flow state file directories, then copy the appropriate files
                if (restoreFlowStateFiles.length == 0 && primaryFlowStateFiles.length != 0) {
                    // copy primary state file to restore
                    FileUtils.copyFile(primaryFlowStateFile, restoreFlowStateFile, false, false, logger);
                } else if (primaryFlowStateFiles.length == 0 && restoreFlowStateFiles.length != 0) {
                    // copy restore state file to primary
                    FileUtils.copyFile(restoreFlowStateFile, primaryFlowStateFile, false, false, logger);
                } else {
                    // sync the primary copy with the restore copy
                    syncWithRestore(primaryFlowStateFile, restoreFlowStateFile);
                }

            }
        } catch (final IOException | IllegalArgumentException | IllegalStateException | JAXBException ex) {
            throw new DaoException(ex);
        }
    }
    
    
    private void syncWithRestore(final File primaryFile, final File restoreFile) throws IOException {
        try (final FileInputStream primaryFis = new FileInputStream(primaryFile);
             final TarArchiveInputStream primaryIn = new TarArchiveInputStream(primaryFis);
             final FileInputStream restoreFis = new FileInputStream(restoreFile);
             final TarArchiveInputStream restoreIn = new TarArchiveInputStream(restoreFis)) {
            
            final ArchiveEntry primaryEntry = primaryIn.getNextEntry();
            final ArchiveEntry restoreEntry = restoreIn.getNextEntry();

            if ( primaryEntry == null && restoreEntry == null ) {
                return;
            }

            if ( (primaryEntry == null && restoreEntry != null) || (primaryEntry != null && restoreEntry == null) ) {
                throw new IllegalStateException(String.format("Primary file '%s' is different than restore file '%s'",
                        primaryFile.getAbsoluteFile(), restoreFile.getAbsolutePath()));
            }
            
            final byte[] primaryMd5 = calculateMd5(primaryIn);
            final byte[] restoreMd5 = calculateMd5(restoreIn);
            
            if ( !Arrays.equals(primaryMd5, restoreMd5) ) {
                throw new IllegalStateException(String.format("Primary file '%s' is different than restore file '%s'",
                        primaryFile.getAbsoluteFile(), restoreFile.getAbsolutePath()));
            }
        }
    }
    
    private byte[] calculateMd5(final InputStream in) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException nsae) {
            throw new IOException(nsae);
        }
        
        int len;
        final byte[] buffer = new byte[8192];
        while ((len = in.read(buffer)) > -1) {
            if (len > 0) {
                digest.update(buffer, 0, len);
            }
        }
        return digest.digest();
    }

    @Override
    public ClusterDataFlow loadDataFlow() throws DaoException {
        try {
            return parseDataFlow(getExistingFlowStateFile(primaryDirectory));
        } catch (final IOException | JAXBException ex) {
            throw new DaoException(ex);
        }
    }

    @Override
    public void saveDataFlow(final ClusterDataFlow dataFlow) throws DaoException {
        try {

            final File primaryStateFile = getFlowStateFile(primaryDirectory);

            // write to restore before writing to primary in case primary experiences problems
            if (restoreDirectory != null) {
                final File restoreStateFile = getFlowStateFile(restoreDirectory);
                if (restoreStateFile == null) {
                    if (primaryStateFile == null) {
                        writeDataFlow(createNewFlowStateFile(restoreDirectory), dataFlow);
                    } else {
                        throw new DaoException(String.format("Unable to save dataflow because dataflow state file in primary directory '%s' exists, but it does not exist in the restore directory '%s'",
                                primaryDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                    }
                } else {
                    if (primaryStateFile == null) {
                        throw new DaoException(String.format("Unable to save dataflow because dataflow state file in restore directory '%s' exists, but it does not exist in the primary directory '%s'",
                                restoreDirectory.getAbsolutePath(), primaryDirectory.getAbsolutePath()));
                    } else {
                        final PersistedFlowState primaryFlowState = getPersistedFlowState(primaryStateFile);
                        final PersistedFlowState restoreFlowState = getPersistedFlowState(restoreStateFile);
                        if (primaryFlowState == restoreFlowState) {
                            writeDataFlow(restoreStateFile, dataFlow);
                        } else {
                            throw new DaoException(String.format("Unable to save dataflow because state file in primary directory '%s' has state '%s', but the state file in the restore directory '%s' has state '%s'",
                                    primaryDirectory.getAbsolutePath(), primaryFlowState, restoreDirectory.getAbsolutePath(), restoreFlowState));
                        }
                    }
                }
            }

            // write dataflow to primary 
            if (primaryStateFile == null) {
                writeDataFlow(createNewFlowStateFile(primaryDirectory), dataFlow);
            } else {
                writeDataFlow(primaryStateFile, dataFlow);
            }

        } catch (final IOException | JAXBException ex) {
            throw new DaoException(ex);
        }
    }

    @Override
    public PersistedFlowState getPersistedFlowState() {
        // trust restore over primary if configured for restore
        if (restoreDirectory == null) {
            return getPersistedFlowState(getExistingFlowStateFile(primaryDirectory));
        } else {
            return getPersistedFlowState(getExistingFlowStateFile(restoreDirectory));
        }
    }

    @Override
    public void setPersistedFlowState(final PersistedFlowState flowState) throws DaoException {
        // rename restore before primary if configured for restore
        if (restoreDirectory != null) {
            renameFlowStateFile(getExistingFlowStateFile(restoreDirectory), flowState);
        }
        renameFlowStateFile(getExistingFlowStateFile(primaryDirectory), flowState);
    }

    private File ensureSingleCurrentStateFile(final File dir) throws IOException, JAXBException {

        // ensure that we have at most one state file and if we have one, it is current
        final File[] directoryFlowStateFiles = getFlowStateFiles(dir);
        if (directoryFlowStateFiles.length > 1) {
            throw new DaoException(String.format("Found multiple dataflow state files in directory '%s'", dir));
        } else if (directoryFlowStateFiles.length == 0) {
            // create a new file if none exist
            return createNewFlowStateFile(dir);
        } else {
            // check that the single flow state file is current
            final PersistedFlowState flowState = getPersistedFlowState(directoryFlowStateFiles[0]);
            if (PersistedFlowState.CURRENT == flowState) {
                return directoryFlowStateFiles[0];
            } else {
                throw new DaoException(String.format("Dataflow state file '%s' must be current.", directoryFlowStateFiles[0].getAbsolutePath()));
            }
        }

    }

    private PersistedFlowState getPersistedFlowState(final File file) {
        final String path = file.getAbsolutePath();
        if (path.endsWith(STALE_EXT)) {
            return PersistedFlowState.STALE;
        } else if (path.endsWith(UNKNOWN_EXT)) {
            return PersistedFlowState.UNKNOWN;
        } else {
            return PersistedFlowState.CURRENT;
        }
    }

    private File getFlowStateFile(final File dir) {
        final File[] files = getFlowStateFiles(dir);
        if (files.length > 1) {
            throw new IllegalStateException(String.format("Expected at most one dataflow state file, but found %s files.", files.length));
        } else if (files.length == 0) {
            return null;
        } else {
            return files[0];
        }
    }

    private File getExistingFlowStateFile(final File dir) {
        final File file = getFlowStateFile(dir);
        if (file == null) {
            throw new IllegalStateException(String.format("Expected a dataflow state file, but none existed in directory '%s'", dir.getAbsolutePath()));
        }
        return file;
    }

    private File[] getFlowStateFiles(final File dir) {
        final File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return (name.equals(FLOW_PACKAGE) || name.endsWith(STALE_EXT) || name.endsWith(UNKNOWN_EXT));
            }
        });

        if (files == null) {
            return new File[0];
        } else {
            return files;
        }
    }

    private File removeStateFileExtension(final File file) {

        final String path = file.getAbsolutePath();
        final int stateFileExtIndex;
        if (path.endsWith(STALE_EXT)) {
            stateFileExtIndex = path.lastIndexOf(STALE_EXT);
        } else if (path.endsWith(UNKNOWN_EXT)) {
            stateFileExtIndex = path.lastIndexOf(UNKNOWN_EXT);
        } else {
            stateFileExtIndex = path.length();
        }

        return new File(path.substring(0, stateFileExtIndex));
    }

    private File addStateFileExtension(final File file, final PersistedFlowState state) {
        switch (state) {
            case CURRENT: {
                return file;
            }
            case STALE: {
                return new File(file.getAbsolutePath() + STALE_EXT);
            }
            case UNKNOWN: {
                return new File(file.getAbsolutePath() + UNKNOWN_EXT);
            }
            default: {
                throw new RuntimeException("Unsupported PersistedFlowState Enum value: " + state);
            }
        }
    }

    private File createNewFlowStateFile(final File dir) throws IOException, JAXBException {
        final File stateFile = new File(dir, FLOW_PACKAGE);
        stateFile.createNewFile();

        final byte[] flowBytes = getEmptyFlowBytes();
        final byte[] templateBytes = new byte[0];
        final byte[] snippetBytes = new byte[0];
        final DataFlow dataFlow = new StandardDataFlow(flowBytes, templateBytes, snippetBytes);

        final ClusterMetadata clusterMetadata = new ClusterMetadata();
        writeDataFlow(stateFile, dataFlow, clusterMetadata);

        return stateFile;
    }

    private byte[] getEmptyFlowBytes() throws IOException {
        try {
            final DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            final Document document = docBuilder.newDocument();

            final Element controller = document.createElement("flowController");
            document.appendChild(controller);

            controller.appendChild(createTextElement(document, "maxThreadCount", "15"));

            final Element rootGroup = document.createElement("rootGroup");
            rootGroup.appendChild(createTextElement(document, "id", generatedRootGroupId));
            rootGroup.appendChild(createTextElement(document, "name", "NiFi Flow"));

            // create the position element
            final Element positionElement = createTextElement(document, "position", "");
            positionElement.setAttribute("x", "0.0");
            positionElement.setAttribute("y", "0.0");
            rootGroup.appendChild(positionElement);

            rootGroup.appendChild(createTextElement(document, "comment", ""));
            controller.appendChild(rootGroup);

            final Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");

            final DOMSource source = new DOMSource(document);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final StreamResult result = new StreamResult(baos);
            transformer.transform(source, result);

            return baos.toByteArray();
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    private Element createTextElement(final Document document, final String elementName, final String value) {
        final Element element = document.createElement(elementName);
        element.setTextContent(value);
        return element;
    }

    private void renameFlowStateFile(final File flowStateFile, final PersistedFlowState newState) throws DaoException {
        final PersistedFlowState existingState = getPersistedFlowState(flowStateFile);
        if (existingState != newState) {
            final File newFlowStateFile = addStateFileExtension(removeStateFileExtension(flowStateFile), newState);
            if (flowStateFile.renameTo(newFlowStateFile) == false) {
                throw new DaoException(
                        String.format("Failed to rename flow state file '%s' to new name '%s'", flowStateFile.getAbsolutePath(), newFlowStateFile.getAbsolutePath()));
            }
        }
    }

    private ClusterDataFlow parseDataFlow(final File file) throws IOException, JAXBException, DaoException {
        byte[] flowBytes = new byte[0];
        byte[] templateBytes = new byte[0];
        byte[] snippetBytes = new byte[0];
        byte[] clusterInfoBytes = new byte[0];

        try (final InputStream inStream = new FileInputStream(file);
                final TarArchiveInputStream tarIn = new TarArchiveInputStream(new BufferedInputStream(inStream))) {
            TarArchiveEntry tarEntry;
            while ((tarEntry = tarIn.getNextTarEntry()) != null) {
                switch (tarEntry.getName()) {
                    case FLOW_XML_FILENAME:
                        flowBytes = new byte[(int) tarEntry.getSize()];
                        StreamUtils.fillBuffer(tarIn, flowBytes, true);
                        break;
                    case TEMPLATES_FILENAME:
                        templateBytes = new byte[(int) tarEntry.getSize()];
                        StreamUtils.fillBuffer(tarIn, templateBytes, true);
                        break;
                    case SNIPPETS_FILENAME:
                        snippetBytes = new byte[(int) tarEntry.getSize()];
                        StreamUtils.fillBuffer(tarIn, snippetBytes, true);
                        break;
                    case CLUSTER_INFO_FILENAME:
                        clusterInfoBytes = new byte[(int) tarEntry.getSize()];
                        StreamUtils.fillBuffer(tarIn, clusterInfoBytes, true);
                        break;
                    default:
                        throw new DaoException("Found Unexpected file in dataflow configuration: " + tarEntry.getName());
                }
            }
        }

        final ClusterMetadata clusterMetadata;
        if (clusterInfoBytes.length == 0) {
            clusterMetadata = null;
        } else {
            final Unmarshaller clusterMetadataUnmarshaller = ClusterMetadata.jaxbCtx.createUnmarshaller();
            clusterMetadata = (ClusterMetadata) clusterMetadataUnmarshaller.unmarshal(new ByteArrayInputStream(clusterInfoBytes));
        }

        final StandardDataFlow dataFlow = new StandardDataFlow(flowBytes, templateBytes, snippetBytes);
        dataFlow.setAutoStartProcessors(autoStart);

        return new ClusterDataFlow(dataFlow, (clusterMetadata == null) ? null : clusterMetadata.getPrimaryNodeId());
    }

    private void writeDataFlow(final File file, final ClusterDataFlow clusterDataFlow) throws IOException, JAXBException {

        // get the data flow
        DataFlow dataFlow = clusterDataFlow.getDataFlow();

        // if no dataflow, then write a new dataflow
        if (dataFlow == null) {
            dataFlow = new StandardDataFlow(new byte[0], new byte[0], new byte[0]);
        }

        // setup the cluster metadata
        final ClusterMetadata clusterMetadata = new ClusterMetadata();
        clusterMetadata.setPrimaryNodeId(clusterDataFlow.getPrimaryNodeId());

        // write to disk
        writeDataFlow(file, dataFlow, clusterMetadata);
    }

    private void writeTarEntry(final TarArchiveOutputStream tarOut, final String filename, final byte[] bytes) throws IOException {
        final TarArchiveEntry flowEntry = new TarArchiveEntry(filename);
        flowEntry.setSize(bytes.length);
        tarOut.putArchiveEntry(flowEntry);
        tarOut.write(bytes);
        tarOut.closeArchiveEntry();
    }

    private void writeDataFlow(final File file, final DataFlow dataFlow, final ClusterMetadata clusterMetadata) throws IOException, JAXBException {

        try (final OutputStream fos = new FileOutputStream(file);
                final TarArchiveOutputStream tarOut = new TarArchiveOutputStream(new BufferedOutputStream(fos))) {

            writeTarEntry(tarOut, FLOW_XML_FILENAME, dataFlow.getFlow());
            writeTarEntry(tarOut, TEMPLATES_FILENAME, dataFlow.getTemplates());
            writeTarEntry(tarOut, SNIPPETS_FILENAME, dataFlow.getSnippets());

            final ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            writeClusterMetadata(clusterMetadata, baos);
            final byte[] clusterInfoBytes = baos.toByteArray();

            writeTarEntry(tarOut, CLUSTER_INFO_FILENAME, clusterInfoBytes);
        }
    }

    private void writeClusterMetadata(final ClusterMetadata clusterMetadata, final OutputStream os) throws IOException, JAXBException {
        // write cluster metadata to output stream
        final Marshaller marshaller = ClusterMetadata.jaxbCtx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(clusterMetadata, os);
    }

    @XmlRootElement(name = "clusterMetadata")
    private static class ClusterMetadata {

        private NodeIdentifier primaryNodeId;

        private static final JAXBContext jaxbCtx;

        static {
            try {
                jaxbCtx = JAXBContext.newInstance(ClusterMetadata.class);
            } catch (final JAXBException je) {
                throw new RuntimeException(je);
            }
        }

        @XmlJavaTypeAdapter(NodeIdentifierAdapter.class)
        public NodeIdentifier getPrimaryNodeId() {
            return primaryNodeId;
        }

        public void setPrimaryNodeId(final NodeIdentifier primaryNodeId) {
            this.primaryNodeId = primaryNodeId;
        }
    }
}
