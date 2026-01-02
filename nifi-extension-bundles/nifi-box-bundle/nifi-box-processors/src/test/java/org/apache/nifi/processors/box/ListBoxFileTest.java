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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxFolder;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class ListBoxFileTest extends AbstractBoxFileTest implements FileListingTestTrait {

    @Override
    @BeforeEach
    void setUp() throws Exception {

        final ListBoxFile testSubject = new ListBoxFile() {
            @Override
            BoxFolder getFolder(String folderId) {
                return mockBoxFolder;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
        testRunner.setProperty(ListBoxFile.FOLDER_ID, TEST_FOLDER_ID);
    }

    @Test
    void testOutputAsAttributesWhereTimestampIsModifiedTime()  {
        final List<String> pathParts = Arrays.asList("path", "to", "file");
        mockFetchedFileList(TEST_FILE_ID, TEST_FILENAME, pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(ListBoxFile.REL_SUCCESS).getFirst();

        ff0.assertAttributeEquals(ID, TEST_FILE_ID);
        ff0.assertAttributeEquals(CoreAttributes.FILENAME.key(), TEST_FILENAME);
        ff0.assertAttributeEquals(CoreAttributes.PATH.key(), "/path/to/file");
        ff0.assertAttributeEquals(SIZE, valueOf(TEST_SIZE));
        ff0.assertAttributeEquals(TIMESTAMP, valueOf(MODIFIED_TIME));
    }

    @Test
    void testOutputAsContent() throws Exception {
        final List<String> pathParts = Arrays.asList("path", "to", "file");

        addJsonRecordWriterFactory();

        mockFetchedFileList(TEST_FILE_ID, TEST_FILENAME, pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME);

        final List<String> expectedContents = singletonList(
                "[" +
                        "{" +
                        "\"box.id\":\"" + TEST_FILE_ID + "\"," +
                        "\"filename\":\"" + TEST_FILENAME + "\"," +
                        "\"path\":\"/path/to/file\"," +
                        "\"box.size\":" + TEST_SIZE + "," +
                        "\"box.timestamp\":" + MODIFIED_TIME +
                        "}" +
                        "]");


        testRunner.run();

        testRunner.assertContents(ListBoxFile.REL_SUCCESS, expectedContents);
    }

    @Test
    void testMigration() {
        TestRunner runner = TestRunners.newTestRunner(ListBoxFile.class);
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry(AbstractBoxProcessor.OLD_BOX_CLIENT_SERVICE_PROPERTY_NAME, AbstractBoxProcessor.BOX_CLIENT_SERVICE.getName()),
                Map.entry("box-folder-id", ListBoxFile.FOLDER_ID.getName()),
                Map.entry("recursive-search", ListBoxFile.RECURSIVE_SEARCH.getName()),
                Map.entry("min-age", ListBoxFile.MIN_AGE.getName()),
                Map.entry(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, ListBoxFile.TRACKING_STATE_CACHE.getName()),
                Map.entry(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, ListBoxFile.TRACKING_TIME_WINDOW.getName()),
                Map.entry(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, ListBoxFile.INITIAL_LISTING_TARGET.getName()),
                Map.entry("target-system-timestamp-precision", AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION.getName()),
                Map.entry("listing-strategy", AbstractListProcessor.LISTING_STRATEGY.getName()),
                Map.entry("record-writer", AbstractListProcessor.RECORD_WRITER.getName())
        );

        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());

        final Set<String> expectedRemoved = Set.of("Distributed Cache Service");
        assertEquals(expectedRemoved, propertyMigrationResult.getPropertiesRemoved());
    }

    private void addJsonRecordWriterFactory() throws InitializationException {
        final RecordSetWriterFactory recordSetWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record_writer", recordSetWriter);
        testRunner.enableControllerService(recordSetWriter);
        testRunner.setProperty(ListBoxFile.RECORD_WRITER, "record_writer");
    }

    @Override
    public BoxFolder getMockBoxFolder() {
        return mockBoxFolder;
    }
}
