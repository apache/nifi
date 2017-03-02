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

package org.apache.nifi.processors.evtx;

import com.google.common.annotations.VisibleForTesting;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.FileHeader;
import org.apache.nifi.processors.evtx.parser.FileHeaderFactory;
import org.apache.nifi.processors.evtx.parser.MalformedChunkException;
import org.apache.nifi.processors.evtx.parser.Record;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"logs", "windows", "event", "evtx", "message", "file"})
@CapabilityDescription("Parses the contents of a Windows Event Log file (evtx) and writes the resulting XML to the FlowFile")
@ReadsAttributes({
        @ReadsAttribute(attribute = "filename", description = "The filename of the evtx file")
})
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The output filename"),
        @WritesAttribute(attribute = "mime.type", description = "The output filetype (application/xml for success and failure relationships, original value for bad chunk and original relationships)"),
})
public class ParseEvtx extends AbstractProcessor {
    public static final String RECORD = "Record";
    public static final String CHUNK = "Chunk";
    public static final String FILE = "File";
    public static final String EVTX_EXTENSION = ".evtx";
    public static final String XML_EXTENSION = ".xml";

    @VisibleForTesting
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that was successfully converted from evtx to XML")
            .build();

    @VisibleForTesting
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that encountered an exception during conversion will be transferred to this relationship with as much parsing as possible done")
            .build();

    @VisibleForTesting
    static final Relationship REL_BAD_CHUNK = new Relationship.Builder()
            .name("bad chunk")
            .description("Any bad chunks of records will be transferred to this relationship in their original binary form")
            .build();

    @VisibleForTesting
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The unmodified input FlowFile will be transferred to this relationship")
            .build();

    @VisibleForTesting
    static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL, REL_BAD_CHUNK)));

    @VisibleForTesting
    static final PropertyDescriptor GRANULARITY = new PropertyDescriptor.Builder()
            .required(true)
            .name("granularity")
            .displayName("Granularity")
            .description("Output flow file for each Record, Chunk, or File encountered in the event log")
            .defaultValue(CHUNK)
            .allowableValues(RECORD, CHUNK, FILE)
            .build();

    @VisibleForTesting
    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(GRANULARITY));

    private final FileHeaderFactory fileHeaderFactory;
    private final MalformedChunkHandler malformedChunkHandler;
    private final RootNodeHandlerFactory rootNodeHandlerFactory;
    private final ResultProcessor resultProcessor;

    public ParseEvtx() {
        this(FileHeader::new, new MalformedChunkHandler(REL_BAD_CHUNK), XmlRootNodeHandler::new, new ResultProcessor(REL_SUCCESS, REL_FAILURE));
    }

    public ParseEvtx(FileHeaderFactory fileHeaderFactory, MalformedChunkHandler malformedChunkHandler, RootNodeHandlerFactory rootNodeHandlerFactory, ResultProcessor resultProcessor) {
        this.fileHeaderFactory = fileHeaderFactory;
        this.malformedChunkHandler = malformedChunkHandler;
        this.rootNodeHandlerFactory = rootNodeHandlerFactory;
        this.resultProcessor = resultProcessor;
    }

    protected String getName(String basename, Object chunkNumber, Object recordNumber, String extension) {
        StringBuilder stringBuilder = new StringBuilder(basename);
        if (chunkNumber != null) {
            stringBuilder.append("-chunk");
            stringBuilder.append(chunkNumber);
        }
        if (recordNumber != null) {
            stringBuilder.append("-record");
            stringBuilder.append(recordNumber);
        }
        stringBuilder.append(extension);
        return stringBuilder.toString();
    }

    protected String getBasename(FlowFile flowFile, ComponentLog logger) {
        String basename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        if (basename.endsWith(EVTX_EXTENSION)) {
            return basename.substring(0, basename.length() - EVTX_EXTENSION.length());
        } else {
            logger.warn("Trying to parse file without .evtx extension {} from flowfile {}", new Object[]{basename, flowFile});
            return basename;
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        ComponentLog logger = getLogger();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String basename = getBasename(flowFile, logger);
        String granularity = context.getProperty(GRANULARITY).getValue();
        if (FILE.equals(granularity)) {
            // File granularity will emit a FlowFile for each input
            FlowFile original = session.clone(flowFile);
            AtomicReference<Exception> exceptionReference = new AtomicReference<>(null);
            FlowFile updated = session.write(flowFile, (in, out) -> {
                processFileGranularity(session, logger, original, basename, exceptionReference, in, out);
            });
            session.transfer(original, REL_ORIGINAL);
            resultProcessor.process(session, logger, updated, exceptionReference.get(), getName(basename, null, null, XML_EXTENSION));
        } else {
            session.read(flowFile, in -> {
                if (RECORD.equals(granularity)) {
                    // Record granularity will emit a FlowFile for every record (event)
                    processRecordGranularity(session, logger, flowFile, basename, in);
                } else if (CHUNK.equals(granularity)) {
                    // Chunk granularity will emit a FlowFile for each chunk of the file
                    processChunkGranularity(session, logger, flowFile, basename, in);
                }
            });
            session.transfer(flowFile, REL_ORIGINAL);
        }
    }

    protected void processFileGranularity(ProcessSession session, ComponentLog componentLog, FlowFile original, String basename,
                                          AtomicReference<Exception> exceptionReference, InputStream in, OutputStream out) throws IOException {
        FileHeader fileHeader = fileHeaderFactory.create(in, componentLog);
        try (RootNodeHandler rootNodeHandler = rootNodeHandlerFactory.create(out)) {
            while (fileHeader.hasNext()) {
                try {
                    ChunkHeader chunkHeader = fileHeader.next();
                    try {
                        while (chunkHeader.hasNext()) {
                            rootNodeHandler.handle(chunkHeader.next().getRootNode());
                        }
                    } catch (IOException e) {
                        malformedChunkHandler.handle(original, session, getName(basename, chunkHeader.getChunkNumber(), null, EVTX_EXTENSION), chunkHeader.getBinaryReader().getBytes());
                        exceptionReference.set(e);
                    }
                } catch (MalformedChunkException e) {
                    malformedChunkHandler.handle(original, session, getName(basename, e.getChunkNum(), null, EVTX_EXTENSION), e.getBadChunk());
                }
            }
        } catch (IOException e) {
            exceptionReference.set(e);
        }
    }

    protected void processChunkGranularity(ProcessSession session, ComponentLog componentLog, FlowFile flowFile, String basename, InputStream in) throws IOException {
        FileHeader fileHeader = fileHeaderFactory.create(in, componentLog);
        while (fileHeader.hasNext()) {
            try {
                ChunkHeader chunkHeader = fileHeader.next();
                FlowFile updated = session.create(flowFile);
                AtomicReference<Exception> exceptionReference = new AtomicReference<>(null);
                updated = session.write(updated, out -> {
                    try (RootNodeHandler rootNodeHandler = rootNodeHandlerFactory.create(out)) {
                        while (chunkHeader.hasNext()) {
                            try {
                                rootNodeHandler.handle(chunkHeader.next().getRootNode());
                            } catch (IOException e) {
                                exceptionReference.set(e);
                                break;
                            }
                        }
                    } catch (IOException e) {
                        exceptionReference.set(e);
                    }
                });
                Exception exception = exceptionReference.get();
                resultProcessor.process(session, componentLog, updated, exception, getName(basename, chunkHeader.getChunkNumber(), null, XML_EXTENSION));
                if (exception != null) {
                    malformedChunkHandler.handle(flowFile, session, getName(basename, chunkHeader.getChunkNumber(), null, EVTX_EXTENSION), chunkHeader.getBinaryReader().getBytes());
                }
            } catch (MalformedChunkException e) {
                malformedChunkHandler.handle(flowFile, session, getName(basename, e.getChunkNum(), null, EVTX_EXTENSION), e.getBadChunk());
            }
        }
    }

    protected void processRecordGranularity(ProcessSession session, ComponentLog componentLog, FlowFile flowFile, String basename, InputStream in) throws IOException {
        FileHeader fileHeader = fileHeaderFactory.create(in, componentLog);
        while (fileHeader.hasNext()) {
            try {
                ChunkHeader chunkHeader = fileHeader.next();
                while (chunkHeader.hasNext()) {
                    FlowFile updated = session.create(flowFile);
                    AtomicReference<Exception> exceptionReference = new AtomicReference<>(null);
                    try {
                        Record record = chunkHeader.next();
                        updated = session.write(updated, out -> {
                            try (RootNodeHandler rootNodeHandler = rootNodeHandlerFactory.create(out)) {
                                try {
                                    rootNodeHandler.handle(record.getRootNode());
                                } catch (IOException e) {
                                    exceptionReference.set(e);
                                }
                            } catch (IOException e) {
                                exceptionReference.set(e);
                            }
                        });
                        resultProcessor.process(session, componentLog, updated, exceptionReference.get(), getName(basename, chunkHeader.getChunkNumber(), record.getRecordNum(), XML_EXTENSION));
                    } catch (Exception e) {
                        exceptionReference.set(e);
                        session.remove(updated);
                    }
                    if (exceptionReference.get() != null) {
                        malformedChunkHandler.handle(flowFile, session, getName(basename, chunkHeader.getChunkNumber(), null, EVTX_EXTENSION), chunkHeader.getBinaryReader().getBytes());
                    }
                }
            } catch (MalformedChunkException e) {
                malformedChunkHandler.handle(flowFile, session, getName(basename, e.getChunkNum(), null, EVTX_EXTENSION), e.getBadChunk());
            }
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }
}
