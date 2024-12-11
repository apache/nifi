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

package org.apache.nifi.tests.system.python;

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PythonNarIT extends NiFiSystemIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createPythonicInstanceFactory();
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testRunProcessorWithBundledDependencies() throws IOException, NiFiClientException, InterruptedException {
        final File nifiHome = getNiFiInstance().getInstanceDirectory();
        final File lib = new File(nifiHome, "lib");
        final File pythonNars = new File(lib, "python-nars");
        final File[] narFiles = pythonNars.listFiles((dir, name) -> name.endsWith(".nar"));
        assertNotNull(narFiles);

        // Copy the python nar files to the lib directory so that they will be loaded on restart.
        for (final File narFile : narFiles) {
            Files.copy(narFile.toPath(), lib.toPath().resolve(narFile.getName()), StandardCopyOption.REPLACE_EXISTING);
        }

        // Delete all python extensions from the python/extensions directory to ensure they are not loaded.
        final File pythonExtensions = new File(nifiHome, "python/extensions");
        final File[] extensionFiles = pythonExtensions.listFiles();
        assertNotNull(extensionFiles);
        for (final File extensionFile : extensionFiles) {
            deleteRecursively(extensionFile);
        }

        // Restart NiFi
        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        // Create instance of the WriteBech32Charset processor, and connect a GenerateFlowFile to it
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity writeVersion = getClientUtil().createPythonProcessor("WriteBech32Charset");
        writeVersion = getClientUtil().setAutoTerminatedRelationships(writeVersion, "failure");
        final ConnectionEntity generateToWriteVersion = getClientUtil().createConnection(generate, writeVersion, "success");

        // Create a TerminateFlowFile processor and connect the WriteNumpyVersionBundledDependency processor to it
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity writeVersionToTerminate = getClientUtil().createConnection(writeVersion, terminate, "success");

        // Wait for processor validation to complete
        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(writeVersion.getId());

        // Run the flow
        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToWriteVersion.getId(), 1);
        getClientUtil().startProcessor(writeVersion);
        waitForQueueCount(writeVersionToTerminate.getId(), 1);

        // Verify the output
        final String contents = getClientUtil().getFlowFileContentAsUtf8(writeVersionToTerminate.getId(), 0);
        // Ensure that the contents written to the FlowFile are the 32 characters used by the bech32 encoding
        assertEquals("qpzry9x8gf2tvdw0s3jn54khce6mua7l", contents);
    }

    private void deleteRecursively(final File file) {
        if (file.isDirectory()) {
            final File[] children = file.listFiles();
            assertNotNull(children);

            for (final File child : children) {
                deleteRecursively(child);
            }
        }
        assertTrue(file.delete());
    }
}
