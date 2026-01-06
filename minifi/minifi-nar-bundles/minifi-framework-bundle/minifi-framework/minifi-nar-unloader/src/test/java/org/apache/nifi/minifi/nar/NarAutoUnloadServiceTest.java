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

package org.apache.nifi.minifi.nar;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.Set;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NarAutoUnloadServiceTest {

    private static final String SUPPORTED_FILENAME = "test.nar";
    private static final String UNPACKED_POSTFIX = "-unpacked";

    @Mock
    private ExtensionManager extensionManager;
    @Mock
    private NarLoader narLoader;
    private File extensionWorkDirectory;

    private NarAutoUnloadService victim;

    @BeforeEach
    public void initTest() {
        extensionWorkDirectory = new File(".");
        victim = new NarAutoUnloadService(extensionManager, extensionWorkDirectory, narLoader);
    }

    @ParameterizedTest
    @MethodSource("notSupportedFileNames")
    public void testNotSupportedFileDoesNotTriggerUnload(String fileName) {

        victim.unloadNarFile(fileName);

        verifyNoInteractions(extensionManager, narLoader);
    }

    @Test
    public void testUnload() {
        Bundle bundle = mock(Bundle.class);
        BundleDetails bundleDetails = mock(BundleDetails.class);
        File workingDirectory = new File(extensionWorkDirectory, SUPPORTED_FILENAME + UNPACKED_POSTFIX);

        when(extensionManager.getAllBundles()).thenReturn(Set.of(bundle));
        when(bundle.getBundleDetails()).thenReturn(bundleDetails);
        when(bundleDetails.getWorkingDirectory()).thenReturn(workingDirectory);

        victim.unloadNarFile(SUPPORTED_FILENAME);

        verify(narLoader).unload(bundle);
    }

    private static Stream<Arguments> notSupportedFileNames() {
        return Stream.of(
                Arguments.of("nar.jar"),
                Arguments.of(".test.nar")
        );
    }
}
