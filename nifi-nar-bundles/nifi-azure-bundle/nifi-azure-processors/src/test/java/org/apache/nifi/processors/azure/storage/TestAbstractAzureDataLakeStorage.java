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
package org.apache.nifi.processors.azure.storage;

import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.ADLS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.DIRECTORY;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.FILE;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.FILESYSTEM;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.nifi.services.azure.storage.ADLSCredentialsService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestAbstractAzureDataLakeStorage {

    private TestRunner runner;

    @Before
    public void setUp() throws Exception {
        // test the property validation in the abstract class via the put processor
        runner = TestRunners.newTestRunner(PutAzureDataLakeStorage.class);

        ADLSCredentialsService credentialsService = mock(ADLSCredentialsService.class);
        when(credentialsService.getIdentifier()).thenReturn("credentials_service");
        runner.addControllerService("credentials_service", credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(FILESYSTEM, "filesystem");
        runner.setProperty(DIRECTORY, "directory");
        runner.setProperty(FILE, "file");
        runner.setProperty(ADLS_CREDENTIALS_SERVICE, "credentials_service");
    }

    @Test
    public void testValid() {
        runner.assertValid();
    }

    @Test
    public void testNotValidWhenNoFilesystemSpecified() {
        runner.removeProperty(FILESYSTEM);

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWhenFilesystemIsEmptyString() {
        runner.setProperty(FILESYSTEM, "");

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWhenNoDirectorySpecified() {
        runner.removeProperty(DIRECTORY);

        runner.assertNotValid();
    }

    @Test
    public void testValidWhenDirectoryIsEmptyString() {
        // the empty string is for the filesystem root directory
        runner.setProperty(DIRECTORY, "");

        runner.assertValid();
    }

    @Test
    public void testNotValidWhenDirectoryIsSlash() {
        runner.setProperty(DIRECTORY, "/");

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWhenDirectoryStartsWithSlash() {
        runner.setProperty(DIRECTORY, "/directory");

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWhenDirectoryIsWhitespaceOnly() {
        runner.setProperty(DIRECTORY, "   ");

        runner.assertNotValid();
    }

    @Test
    public void testValidWhenNoFileSpecified() {
        // the default value will be used
        runner.removeProperty(FILE);

        runner.assertValid();
    }

    @Test
    public void testNotValidWhenFileIsEmptyString() {
        runner.setProperty(FILE, "");

        runner.assertNotValid();
    }
}
