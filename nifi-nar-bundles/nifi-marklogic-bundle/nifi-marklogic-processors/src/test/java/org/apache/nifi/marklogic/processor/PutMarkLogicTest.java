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
package org.apache.nifi.marklogic.processor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;

public class PutMarkLogicTest extends AbstractMarkLogicProcessorTest {

    private TestPutMarkLogic processor;

    @Before
    public void setup() throws InitializationException {
        processor = new TestPutMarkLogic();
        initialize(processor);
    }

    @Test
    public void jsonWithCustomCollections() {
        processContext.setProperty(PutMarkLogic.COLLECTIONS, " collection1, collection2,collection3,   collection4   ");
        processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
        processor.initialize(initializationContext);

        addFlowFile("{\"hello\":\"nifi rocks\"}");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        assertEquals(2, processor.relationships.size());
        assertFalse("flushAsync should not have been called yet since a FlowFile existed in the session", processor.flushAsyncCalled);

        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals(Format.JSON, content.getFormat());
        assertEquals("{\"hello\":\"nifi rocks\"}", new String(content.get()));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) processor.writeEvent.getMetadata();
        assertEquals(4, metadata.getCollections().size());
        Iterator<String> collections = metadata.getCollections().iterator();
        assertEquals("collection1", collections.next());
        assertEquals("collection2", collections.next());
        assertEquals("collection3", collections.next());
        assertEquals("collection4", collections.next());
    }

    @Test
    public void jsonWithNoCollections() {
        processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
        processor.initialize(initializationContext);

        addFlowFile("{\"hello\":\"nifi rocks\"}");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        assertEquals(2, processor.relationships.size());
        assertFalse("flushAsync should not have been called yet since a FlowFile existed in the session", processor.flushAsyncCalled);

        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals(Format.JSON, content.getFormat());
        assertEquals("{\"hello\":\"nifi rocks\"}", new String(content.get()));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) processor.writeEvent.getMetadata();
        assertEquals(0, metadata.getCollections().size());
    }

    @Test
    public void jsonWithEmptyCollections() {
        processContext.setProperty(PutMarkLogic.COLLECTIONS, "");
        processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
        processor.initialize(initializationContext);

        addFlowFile("{\"hello\":\"nifi rocks\"}");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        assertEquals(2, processor.relationships.size());
        assertFalse("flushAsync should not have been called yet since a FlowFile existed in the session", processor.flushAsyncCalled);

        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals(Format.JSON, content.getFormat());
        assertEquals("{\"hello\":\"nifi rocks\"}", new String(content.get()));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) processor.writeEvent.getMetadata();
        assertEquals(0, metadata.getCollections().size());
    }

    @Test
    public void jsonWithEmptyStringCollections() {
        processContext.setProperty(PutMarkLogic.COLLECTIONS, "  ");
        processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
        processor.initialize(initializationContext);

        addFlowFile("{\"hello\":\"nifi rocks\"}");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        assertEquals(2, processor.relationships.size());
        assertFalse("flushAsync should not have been called yet since a FlowFile existed in the session", processor.flushAsyncCalled);

        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals(Format.JSON, content.getFormat());
        assertEquals("{\"hello\":\"nifi rocks\"}", new String(content.get()));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) processor.writeEvent.getMetadata();
        assertEquals(0, metadata.getCollections().size());
    }

    @Test
    public void jsonWithAttributeValueCollectionsMissingString() {
        // This test simulates the case where we fetch collections from an attribute
        // value specified in a previous step of the flow, but none of the processors in the flow
        // have that attribute. The results should be that documents are created with no collections.
        // The point here is that a mistake (or on purpose) was made in setup where the attribute
        // is missing in the flowfile. But, we do not want the flow to error. We continue as if no
        // collection was specified
        processContext.setProperty(PutMarkLogic.COLLECTIONS, "${CollectionText}");
        processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
        processor.initialize(initializationContext);

        addFlowFile("{\"hello\":\"nifi rocks\"}");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        assertEquals(2, processor.relationships.size());
        assertFalse("flushAsync should not have been called yet since a FlowFile existed in the session", processor.flushAsyncCalled);

        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals(Format.JSON, content.getFormat());
        assertEquals("{\"hello\":\"nifi rocks\"}", new String(content.get()));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) processor.writeEvent.getMetadata();
        assertEquals(0, metadata.getCollections().size());
    }

    @Test
    public void jsonWithSensibleCollectionsString() {
        processContext.setProperty(PutMarkLogic.COLLECTIONS, "foo,bar");
        processContext.setProperty(PutMarkLogic.FORMAT, Format.JSON.name());
        processor.initialize(initializationContext);

        addFlowFile("{\"hello\":\"nifi rocks\"}");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        assertEquals(2, processor.relationships.size());
        assertFalse("flushAsync should not have been called yet since a FlowFile existed in the session", processor.flushAsyncCalled);

        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals(Format.JSON, content.getFormat());
        assertEquals("{\"hello\":\"nifi rocks\"}", new String(content.get()));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) processor.writeEvent.getMetadata();
        assertEquals(2, metadata.getCollections().size());
        Iterator<String> collections = metadata.getCollections().iterator();
        // Assertion check order based on alphabetical order of the collection name
        assertEquals("bar", collections.next());
        assertEquals("foo", collections.next());
    }

    @Test
    public void customPermissions() {
        processContext.setProperty(PutMarkLogic.PERMISSIONS, "manage-user,read,manage-admin,update");
        processor.initialize(initializationContext);

        addFlowFile("<test/>");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) processor.writeEvent.getMetadata();
        DocumentMetadataHandle.DocumentPermissions perms = metadata.getPermissions();
        assertEquals(2, perms.size());
        assertEquals(DocumentMetadataHandle.Capability.READ, perms.get("manage-user").iterator().next());
        assertEquals(DocumentMetadataHandle.Capability.UPDATE, perms.get("manage-admin").iterator().next());
    }

    @Test
    public void customMimetype() {
        processContext.setProperty(PutMarkLogic.MIMETYPE, "text/xml");
        processor.initialize(initializationContext);

        addFlowFile("<test/>");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals("text/xml", content.getMimetype());
        assertEquals("The format defaults to UNKNOWN when it's not set", Format.UNKNOWN, content.getFormat());
    }

    @Test
    public void xmlWithCustomUri() {
        processContext.setProperty(PutMarkLogic.FORMAT, Format.XML.name());
        processContext.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "someNumber");
        processContext.setProperty(PutMarkLogic.URI_PREFIX, "/prefix/");
        processContext.setProperty(PutMarkLogic.URI_SUFFIX, "/suffix.xml");
        processor.initialize(initializationContext);

        MockFlowFile flowFile = addFlowFile("<test/>");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("someNumber", "12345");
        flowFile.putAttributes(attributes);

        processor.onTrigger(processContext, mockProcessSessionFactory);

        assertEquals("/prefix/12345/suffix.xml", processor.writeEvent.getTargetUri());

        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals(Format.XML, content.getFormat());
        assertEquals("<test/>", new String(content.get()));
    }

    @Test
    public void noFlowFileExists() {
        processor.onTrigger(processContext, mockProcessSessionFactory);
        assertTrue(
            "When no FlowFile exists in the session, flushAsync should be called on the WriteBatcher so that any documents that " +
                "haven't been written to ML yet can be flushed",
            processor.flushAsyncCalled
        );
    }

    private void addFlowFileWithName(String content, String fileName) {
        MockFlowFile flowFile = addFlowFile(content);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", fileName);
        flowFile.putAttributes(attributes);
    }

    private void checkContentFormat(Format format) {
        BytesHandle content = (BytesHandle) processor.writeEvent.getContent();
        assertEquals(format, content.getFormat());
    }

    @Test
    public void checkXMLFormat() {
        processContext.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "filename");
        processor.initialize(initializationContext);

        // Sample XML File
        addFlowFileWithName("<test/>", "sample.xml");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        checkContentFormat(Format.XML);
    }

    @Test
    public void checkJSONFormat() {
        processContext.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "filename");
        processor.initialize(initializationContext);

        // Sample JSON File
        addFlowFileWithName("{\"test\":\"file\"}", "sample.json");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        checkContentFormat(Format.JSON);
    }

    @Test
    public void checkTextFormat() {
        processContext.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "filename");
        processor.initialize(initializationContext);

        // Sample TEXT File
        addFlowFileWithName("Simple text document", "sample.txt");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        checkContentFormat(Format.TEXT);
    }

    @Test
    public void checkDefaultFormat() {
        processContext.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "filename");
        processor.initialize(initializationContext);
        // Sample Random File
        addFlowFileWithName("Simple text document", "sample");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        checkContentFormat(Format.UNKNOWN);
    }
}

/**
 * This subclass allows us to intercept the calls to WriteBatcher so that no calls are made to MarkLogic.
 */
class TestPutMarkLogic extends PutMarkLogic {

    public boolean flushAsyncCalled = false;
    public WriteEvent writeEvent;

    @Override
    protected void flushWriteBatcherAsync(WriteBatcher writeBatcher) {
        flushAsyncCalled = true;
    }

    @Override
    protected void addWriteEvent(WriteBatcher writeBatcher, WriteEvent writeEvent) {
        this.writeEvent = writeEvent;
    }
}
