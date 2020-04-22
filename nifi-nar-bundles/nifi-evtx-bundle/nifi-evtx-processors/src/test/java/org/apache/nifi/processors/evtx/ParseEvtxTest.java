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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.FileHeader;
import org.apache.nifi.processors.evtx.parser.FileHeaderFactory;
import org.apache.nifi.processors.evtx.parser.MalformedChunkException;
import org.apache.nifi.processors.evtx.parser.Record;
import org.apache.nifi.processors.evtx.parser.bxml.RootNode;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

@RunWith(MockitoJUnitRunner.class)
public class ParseEvtxTest {
    public static final String USER_DATA = "UserData";
    public static final String EVENT_DATA = "EventData";
    public static final Set DATA_TAGS = new HashSet<>(Arrays.asList(EVENT_DATA, USER_DATA));
    public static final int EXPECTED_SUCCESSFUL_EVENT_COUNT = 1053;

    @Mock
    FileHeaderFactory fileHeaderFactory;

    @Mock
    MalformedChunkHandler malformedChunkHandler;

    @Mock
    RootNodeHandlerFactory rootNodeHandlerFactory;

    @Mock
    ResultProcessor resultProcessor;

    @Mock
    ComponentLog componentLog;

    @Mock
    InputStream in;

    @Mock
    OutputStream out;

    @Mock
    FileHeader fileHeader;

    ParseEvtx parseEvtx;

    @Before
    public void setup() throws XMLStreamException, IOException {
        parseEvtx = new ParseEvtx(fileHeaderFactory, malformedChunkHandler, rootNodeHandlerFactory, resultProcessor);
        when(fileHeaderFactory.create(in, componentLog)).thenReturn(fileHeader);
    }

    @Test
    public void testGetNameFile() {
        String basename = "basename";
        assertEquals(basename + ".xml", parseEvtx.getName(basename, null, null, ParseEvtx.XML_EXTENSION));
    }

    @Test
    public void testGetNameFileChunk() {
        String basename = "basename";
        assertEquals(basename + "-chunk1.xml", parseEvtx.getName(basename, 1, null, ParseEvtx.XML_EXTENSION));
    }

    @Test
    public void testGetNameFileChunkRecord() {
        String basename = "basename";
        assertEquals(basename + "-chunk1-record2.xml", parseEvtx.getName(basename, 1, 2, ParseEvtx.XML_EXTENSION));
    }

    @Test
    public void testGetBasenameEvtxExtension() {
        String basename = "basename";
        FlowFile flowFile = mock(FlowFile.class);

        when(flowFile.getAttribute(CoreAttributes.FILENAME.key())).thenReturn(basename + ".evtx");

        assertEquals(basename, parseEvtx.getBasename(flowFile, componentLog));
        verifyNoMoreInteractions(componentLog);
    }

    @Test
    public void testGetBasenameExtension() {
        String basename = "basename.wrongextension";
        FlowFile flowFile = mock(FlowFile.class);
        ComponentLog componentLog = mock(ComponentLog.class);

        when(flowFile.getAttribute(CoreAttributes.FILENAME.key())).thenReturn(basename);

        assertEquals(basename, parseEvtx.getBasename(flowFile, componentLog));
    }

    @Test
    public void testGetRelationships() {
        assertEquals(ParseEvtx.RELATIONSHIPS, parseEvtx.getRelationships());
    }

    @Test
    public void testGetSupportedPropertyDescriptors() {
        assertEquals(ParseEvtx.PROPERTY_DESCRIPTORS, parseEvtx.getSupportedPropertyDescriptors());
    }

    @Test
    public void testProcessFileGranularity() throws IOException, MalformedChunkException, XMLStreamException {
        String basename = "basename";
        int chunkNum = 5;
        int offset = 10001;
        byte[] badChunk = {8};

        RootNodeHandler rootNodeHandler = mock(RootNodeHandler.class);
        when(rootNodeHandlerFactory.create(out)).thenReturn(rootNodeHandler);
        ChunkHeader chunkHeader1 = mock(ChunkHeader.class);
        ChunkHeader chunkHeader2 = mock(ChunkHeader.class);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        Record record3 = mock(Record.class);
        RootNode rootNode1 = mock(RootNode.class);
        RootNode rootNode2 = mock(RootNode.class);
        RootNode rootNode3 = mock(RootNode.class);
        ProcessSession session = mock(ProcessSession.class);
        FlowFile flowFile = mock(FlowFile.class);
        AtomicReference<Exception> reference = new AtomicReference<>();
        MalformedChunkException malformedChunkException = new MalformedChunkException("Test", null, offset, chunkNum, badChunk);

        when(record1.getRootNode()).thenReturn(rootNode1);
        when(record2.getRootNode()).thenReturn(rootNode2);
        when(record3.getRootNode()).thenReturn(rootNode3);

        when(fileHeader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(fileHeader.next()).thenThrow(malformedChunkException).thenReturn(chunkHeader1).thenReturn(chunkHeader2).thenReturn(null);

        when(chunkHeader1.hasNext()).thenReturn(true).thenReturn(false);
        when(chunkHeader1.next()).thenReturn(record1).thenReturn(null);

        when(chunkHeader2.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(chunkHeader2.next()).thenReturn(record2).thenReturn(record3).thenReturn(null);

        parseEvtx.processFileGranularity(session, componentLog, flowFile, basename, reference, in, out);

        verify(malformedChunkHandler).handle(flowFile, session, parseEvtx.getName(basename, chunkNum, null, ParseEvtx.EVTX_EXTENSION), badChunk);
        verify(rootNodeHandler).handle(rootNode1);
        verify(rootNodeHandler).handle(rootNode2);
        verify(rootNodeHandler).handle(rootNode3);
        verify(rootNodeHandler).close();
    }

    @Test
    public void testProcessChunkGranularity() throws IOException, MalformedChunkException, XMLStreamException {
        String basename = "basename";
        int chunkNum = 5;
        int offset = 10001;
        byte[] badChunk = {8};

        RootNodeHandler rootNodeHandler1 = mock(RootNodeHandler.class);
        RootNodeHandler rootNodeHandler2 = mock(RootNodeHandler.class);
        OutputStream out2 = mock(OutputStream.class);
        when(rootNodeHandlerFactory.create(out)).thenReturn(rootNodeHandler1);
        when(rootNodeHandlerFactory.create(out2)).thenReturn(rootNodeHandler2);
        ChunkHeader chunkHeader1 = mock(ChunkHeader.class);
        ChunkHeader chunkHeader2 = mock(ChunkHeader.class);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        Record record3 = mock(Record.class);
        RootNode rootNode1 = mock(RootNode.class);
        RootNode rootNode2 = mock(RootNode.class);
        RootNode rootNode3 = mock(RootNode.class);
        ProcessSession session = mock(ProcessSession.class);
        FlowFile flowFile = mock(FlowFile.class);
        FlowFile created1 = mock(FlowFile.class);
        FlowFile updated1 = mock(FlowFile.class);
        FlowFile created2 = mock(FlowFile.class);
        FlowFile updated2 = mock(FlowFile.class);
        MalformedChunkException malformedChunkException = new MalformedChunkException("Test", null, offset, chunkNum, badChunk);

        when(session.create(flowFile)).thenReturn(created1).thenReturn(created2).thenReturn(null);

        when(session.write(eq(created1), any(OutputStreamCallback.class))).thenAnswer(invocation -> {
            ((OutputStreamCallback) invocation.getArguments()[1]).process(out);
            return updated1;
        });

        when(session.write(eq(created2), any(OutputStreamCallback.class))).thenAnswer(invocation -> {
            ((OutputStreamCallback) invocation.getArguments()[1]).process(out2);
            return updated2;
        });

        when(record1.getRootNode()).thenReturn(rootNode1);
        when(record2.getRootNode()).thenReturn(rootNode2);
        when(record3.getRootNode()).thenReturn(rootNode3);

        when(fileHeader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(fileHeader.next()).thenThrow(malformedChunkException).thenReturn(chunkHeader1).thenReturn(chunkHeader2).thenReturn(null);

        when(chunkHeader1.hasNext()).thenReturn(true).thenReturn(false);
        when(chunkHeader1.next()).thenReturn(record1).thenReturn(null);

        when(chunkHeader2.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(chunkHeader2.next()).thenReturn(record2).thenReturn(record3).thenReturn(null);

        parseEvtx.processChunkGranularity(session, componentLog, flowFile, basename, in);

        verify(malformedChunkHandler).handle(flowFile, session, parseEvtx.getName(basename, chunkNum, null, ParseEvtx.EVTX_EXTENSION), badChunk);
        verify(rootNodeHandler1).handle(rootNode1);
        verify(rootNodeHandler1).close();
        verify(rootNodeHandler2).handle(rootNode2);
        verify(rootNodeHandler2).handle(rootNode3);
        verify(rootNodeHandler2).close();
    }

    @Test
    public void testProcess1RecordGranularity() throws IOException, MalformedChunkException, XMLStreamException {
        String basename = "basename";
        int chunkNum = 5;
        int offset = 10001;
        byte[] badChunk = {8};

        RootNodeHandler rootNodeHandler1 = mock(RootNodeHandler.class);
        RootNodeHandler rootNodeHandler2 = mock(RootNodeHandler.class);
        RootNodeHandler rootNodeHandler3 = mock(RootNodeHandler.class);
        OutputStream out2 = mock(OutputStream.class);
        OutputStream out3 = mock(OutputStream.class);
        when(rootNodeHandlerFactory.create(out)).thenReturn(rootNodeHandler1);
        when(rootNodeHandlerFactory.create(out2)).thenReturn(rootNodeHandler2);
        when(rootNodeHandlerFactory.create(out3)).thenReturn(rootNodeHandler3);
        ChunkHeader chunkHeader1 = mock(ChunkHeader.class);
        ChunkHeader chunkHeader2 = mock(ChunkHeader.class);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        Record record3 = mock(Record.class);
        RootNode rootNode1 = mock(RootNode.class);
        RootNode rootNode2 = mock(RootNode.class);
        RootNode rootNode3 = mock(RootNode.class);
        ProcessSession session = mock(ProcessSession.class);
        FlowFile flowFile = mock(FlowFile.class);
        FlowFile created1 = mock(FlowFile.class);
        FlowFile updated1 = mock(FlowFile.class);
        FlowFile created2 = mock(FlowFile.class);
        FlowFile updated2 = mock(FlowFile.class);
        FlowFile created3 = mock(FlowFile.class);
        FlowFile updated3 = mock(FlowFile.class);
        MalformedChunkException malformedChunkException = new MalformedChunkException("Test", null, offset, chunkNum, badChunk);

        when(session.create(flowFile)).thenReturn(created1).thenReturn(created2).thenReturn(created3).thenReturn(null);

        when(session.write(eq(created1), any(OutputStreamCallback.class))).thenAnswer(invocation -> {
            ((OutputStreamCallback) invocation.getArguments()[1]).process(out);
            return updated1;
        });

        when(session.write(eq(created2), any(OutputStreamCallback.class))).thenAnswer(invocation -> {
            ((OutputStreamCallback) invocation.getArguments()[1]).process(out2);
            return updated2;
        });

        when(session.write(eq(created3), any(OutputStreamCallback.class))).thenAnswer(invocation -> {
            ((OutputStreamCallback) invocation.getArguments()[1]).process(out3);
            return updated3;
        });

        when(record1.getRootNode()).thenReturn(rootNode1);
        when(record2.getRootNode()).thenReturn(rootNode2);
        when(record3.getRootNode()).thenReturn(rootNode3);

        when(fileHeader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(fileHeader.next()).thenThrow(malformedChunkException).thenReturn(chunkHeader1).thenReturn(chunkHeader2).thenReturn(null);

        when(chunkHeader1.hasNext()).thenReturn(true).thenReturn(false);
        when(chunkHeader1.next()).thenReturn(record1).thenReturn(null);

        when(chunkHeader2.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(chunkHeader2.next()).thenReturn(record2).thenReturn(record3).thenReturn(null);

        parseEvtx.processRecordGranularity(session, componentLog, flowFile, basename, in);

        verify(malformedChunkHandler).handle(flowFile, session, parseEvtx.getName(basename, chunkNum, null, ParseEvtx.EVTX_EXTENSION), badChunk);
        verify(rootNodeHandler1).handle(rootNode1);
        verify(rootNodeHandler1).close();
        verify(rootNodeHandler2).handle(rootNode2);
        verify(rootNodeHandler2).close();
        verify(rootNodeHandler3).handle(rootNode3);
        verify(rootNodeHandler3).close();
    }

    @Test
    public void fileGranularityLifecycleTest() throws IOException, ParserConfigurationException, SAXException {
        String baseName = "testFileName";
        String name = baseName + ".evtx";
        TestRunner testRunner = TestRunners.newTestRunner(ParseEvtx.class);
        testRunner.setProperty(ParseEvtx.GRANULARITY, ParseEvtx.FILE);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), name);
        testRunner.enqueue(this.getClass().getClassLoader().getResourceAsStream("application-logs.evtx"), attributes);
        testRunner.run();

        List<MockFlowFile> originalFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_ORIGINAL);
        assertEquals(1, originalFlowFiles.size());
        MockFlowFile originalFlowFile = originalFlowFiles.get(0);
        originalFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), name);
        originalFlowFile.assertContentEquals(this.getClass().getClassLoader().getResourceAsStream("application-logs.evtx"));

        // We expect the same bad chunks no matter the granularity
        List<MockFlowFile> badChunkFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_BAD_CHUNK);
        assertEquals(2, badChunkFlowFiles.size());
        badChunkFlowFiles.get(0).assertAttributeEquals(CoreAttributes.FILENAME.key(), parseEvtx.getName(baseName, 1, null, ParseEvtx.EVTX_EXTENSION));
        badChunkFlowFiles.get(1).assertAttributeEquals(CoreAttributes.FILENAME.key(), parseEvtx.getName(baseName, 2, null, ParseEvtx.EVTX_EXTENSION));

        List<MockFlowFile> failureFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());
        validateFlowFiles(failureFlowFiles);
        // We expect the same number of records to come out no matter the granularity
        assertEquals(EXPECTED_SUCCESSFUL_EVENT_COUNT, validateFlowFiles(failureFlowFiles));

        // Whole file fails if there is a failure parsing
        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_SUCCESS);
        assertEquals(0, successFlowFiles.size());
    }

    @Test
    public void chunkGranularityLifecycleTest() throws IOException, ParserConfigurationException, SAXException {
        String baseName = "testFileName";
        String name = baseName + ".evtx";
        TestRunner testRunner = TestRunners.newTestRunner(ParseEvtx.class);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), name);
        testRunner.enqueue(this.getClass().getClassLoader().getResourceAsStream("application-logs.evtx"), attributes);
        testRunner.run();

        List<MockFlowFile> originalFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_ORIGINAL);
        assertEquals(1, originalFlowFiles.size());
        MockFlowFile originalFlowFile = originalFlowFiles.get(0);
        originalFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), name);
        originalFlowFile.assertContentEquals(this.getClass().getClassLoader().getResourceAsStream("application-logs.evtx"));

        // We expect the same bad chunks no matter the granularity
        List<MockFlowFile> badChunkFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_BAD_CHUNK);
        assertEquals(2, badChunkFlowFiles.size());
        badChunkFlowFiles.get(0).assertAttributeEquals(CoreAttributes.FILENAME.key(), parseEvtx.getName(baseName, 1, null, ParseEvtx.EVTX_EXTENSION));
        badChunkFlowFiles.get(1).assertAttributeEquals(CoreAttributes.FILENAME.key(), parseEvtx.getName(baseName, 2, null, ParseEvtx.EVTX_EXTENSION));

        List<MockFlowFile> failureFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_SUCCESS);
        assertEquals(9, successFlowFiles.size());

        // We expect the same number of records to come out no matter the granularity
        assertEquals(EXPECTED_SUCCESSFUL_EVENT_COUNT, validateFlowFiles(successFlowFiles) + validateFlowFiles(failureFlowFiles));
    }

    @Test
    public void recordGranularityLifecycleTest() throws IOException, ParserConfigurationException, SAXException {
        String baseName = "testFileName";
        String name = baseName + ".evtx";
        TestRunner testRunner = TestRunners.newTestRunner(ParseEvtx.class);
        testRunner.setProperty(ParseEvtx.GRANULARITY, ParseEvtx.RECORD);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), name);
        testRunner.enqueue(this.getClass().getClassLoader().getResourceAsStream("application-logs.evtx"), attributes);
        testRunner.run();

        List<MockFlowFile> originalFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_ORIGINAL);
        assertEquals(1, originalFlowFiles.size());
        MockFlowFile originalFlowFile = originalFlowFiles.get(0);
        originalFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), name);
        originalFlowFile.assertContentEquals(this.getClass().getClassLoader().getResourceAsStream("application-logs.evtx"));

        // We expect the same bad chunks no matter the granularity
        List<MockFlowFile> badChunkFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_BAD_CHUNK);
        assertEquals(2, badChunkFlowFiles.size());
        badChunkFlowFiles.get(0).assertAttributeEquals(CoreAttributes.FILENAME.key(), parseEvtx.getName(baseName, 1, null, ParseEvtx.EVTX_EXTENSION));
        badChunkFlowFiles.get(1).assertAttributeEquals(CoreAttributes.FILENAME.key(), parseEvtx.getName(baseName, 2, null, ParseEvtx.EVTX_EXTENSION));

        List<MockFlowFile> failureFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        // Whole file fails if there is a failure parsing
        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ParseEvtx.REL_SUCCESS);
        assertEquals(EXPECTED_SUCCESSFUL_EVENT_COUNT, successFlowFiles.size());

        // We expect the same number of records to come out no matter the granularity
        assertEquals(EXPECTED_SUCCESSFUL_EVENT_COUNT, validateFlowFiles(successFlowFiles));
    }

    @Test
    public void testRecordBasedParseCorrectNumberOfFlowFiles() {
        testValidEvents(ParseEvtx.RECORD, "1344_events.evtx", 1344);
    }

    @Test
    public void testChunkBasedParseCorrectNumberOfFlowFiles() {
        testValidEvents(ParseEvtx.CHUNK, "1344_events.evtx", 14);
    }

    @Test
    public void testRecordBasedParseCorrectNumberOfFlowFilesFromAResizedFile() {
        testValidEvents(ParseEvtx.RECORD, "3778_events_not_exported.evtx", 3778);
    }

    @Test
    public void testChunkBasedParseCorrectNumberOfFlowFilesFromAResizedFile() {
        testValidEvents(ParseEvtx.CHUNK, "3778_events_not_exported.evtx", 16);
    }

    private void testValidEvents(String granularity, String filename, int expectedCount) {
        TestRunner testRunner = TestRunners.newTestRunner(ParseEvtx.class);
        testRunner.setProperty(ParseEvtx.GRANULARITY, granularity);
        Map<String, String> attributes = new HashMap<>();
        ClassLoader classLoader = this.getClass().getClassLoader();
        InputStream resourceAsStream = classLoader.getResourceAsStream(filename);
        testRunner.enqueue(resourceAsStream, attributes);
        testRunner.run();

        testRunner.assertTransferCount(ParseEvtx.REL_SUCCESS, expectedCount);
    }

    private int validateFlowFiles(List<MockFlowFile> successFlowFiles) throws SAXException, IOException, ParserConfigurationException {
        assertTrue(successFlowFiles.size() > 0);
        int totalSize = 0;
        for (MockFlowFile successFlowFile : successFlowFiles) {
            // Verify valid XML output
            Document document = XmlUtils.createSafeDocumentBuilder(false).parse(new ByteArrayInputStream(successFlowFile.toByteArray()));
            Element documentElement = document.getDocumentElement();
            assertEquals(XmlRootNodeHandler.EVENTS, documentElement.getTagName());
            NodeList eventNodes = documentElement.getChildNodes();
            int length = eventNodes.getLength();
            totalSize += length;
            assertTrue(length > 0);
            for (int i = 0; i < length; i++) {
                Node eventNode = eventNodes.item(i);
                assertEquals("Event", eventNode.getNodeName());

                NodeList eventChildNodes = eventNode.getChildNodes();
                assertEquals(2, eventChildNodes.getLength());

                Node systemNode = eventChildNodes.item(0);
                assertEquals("System", systemNode.getNodeName());

                NodeList childNodes = systemNode.getChildNodes();
                String userId = "";
                for (int i1 = 0; i1 < childNodes.getLength(); i1++) {
                    Node systemChild = childNodes.item(i1);
                    if ("Security".equals(systemChild.getNodeName())) {
                        userId = systemChild.getAttributes().getNamedItem("UserID").getNodeValue();
                    }
                }

                Node eventDataNode = eventChildNodes.item(1);
                String eventDataNodeNodeName = eventDataNode.getNodeName();
                assertTrue(DATA_TAGS.contains(eventDataNodeNodeName));
                assertTrue(userId.length() == 0 || userId.startsWith("S-"));
            }
        }
        return totalSize;
    }
}
