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

package org.apache.nifi.processors.stateless.retrieval;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.stateless.ExecuteStateless;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;

import java.io.File;
import java.io.IOException;

/**
 * Wrapper that can be used in order to cache a dataflow once it has been fetched for backup purposes.
 * This provider will always first delegate to the given DataflowProvider first. If the given provider is
 * able to retrieve the dataflow, this provider will then store the dataflow is a file. If, later, the
 * given provider is unable to retrieve the dataflow, due to the endpoint being down, etc., then this provider
 * will instead parse the cached file. This eliminates the concern of requiring that some external endpoint is
 * available in order to run the dataflow.
 */
public class CachingDataflowProvider implements DataflowProvider {
    private final String processorId;
    private final ComponentLog logger;
    private final DataflowProvider delegate;
    private final ObjectMapper objectMapper;


    public CachingDataflowProvider(final String processorId, final ComponentLog logger, final DataflowProvider delegate) {
        this.processorId = processorId;
        this.logger = logger;
        this.delegate = delegate;

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(objectMapper.getTypeFactory()));
    }

    @Override
    public VersionedFlowSnapshot retrieveDataflowContents(final ProcessContext context) throws IOException {
        try {
            final VersionedFlowSnapshot retrieved = delegate.retrieveDataflowContents(context);
            cacheFlowSnapshot(context, retrieved);
            return retrieved;
        } catch (final Exception e) {
            final File cacheFile = getFlowCacheFile(context, processorId);
            if (cacheFile.exists()) {
                logger.warn("Failed to retrieve Flow Snapshot. Will restore Flow Snapshot from cached version at {}", cacheFile.getAbsolutePath(), e);
                return readCachedFlow(cacheFile);
            }

            throw new IOException("Failed to retrieve Flow Snapshot from configured endpoint and no cached version is available", e);
        }
    }

    private void cacheFlowSnapshot(final ProcessContext context, final VersionedFlowSnapshot flowSnapshot) {
        final File cacheFile = getFlowCacheFile(context, processorId);
        if (!cacheFile.getParentFile().exists() && !cacheFile.getParentFile().mkdirs()) {
            logger.warn("Fetched dataflow but cannot create directory {} in order to cache the dataflow. " +
                "Upon restart, processor will not be able to function unless flow endpoint is available", cacheFile);
            return;
        }

        try {
            objectMapper.writeValue(cacheFile, flowSnapshot);
        } catch (final Exception e) {
            logger.warn("Fetched dataflow but failed to write the dataflow to disk at {} in order to cache the dataflow. " +
                "Upon restart, processor will not be able to function unless flow endpoint is available", cacheFile, e);
        }
    }

    protected File getFlowCacheFile(final ProcessContext context, final String processorId) {
        final String workingDirName = context.getProperty(ExecuteStateless.WORKING_DIRECTORY).getValue();
        final File workingDir = new File(workingDirName);
        final File dataflowCache = new File(workingDir, "dataflow-cache");
        final File flowSnapshotFile = new File(dataflowCache, processorId + ".flow.snapshot.json");
        return flowSnapshotFile;
    }

    private VersionedFlowSnapshot readCachedFlow(final File cacheFile) throws IOException {
        return objectMapper.readValue(cacheFile, VersionedFlowSnapshot.class);
    }
}
