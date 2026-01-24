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
package org.apache.nifi.tests.system;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * Test Watcher responsible for writing troubleshooting logs.
 * <p>
 * This extension implements {@link AfterTestExecutionCallback} to capture diagnostics from the running
 * NiFi instance BEFORE the {@code @AfterEach} method tears it down. The JUnit 5 execution order is:
 * <ol>
 *   <li>{@code @BeforeEach}</li>
 *   <li>Test method (may fail)</li>
 *   <li>{@code AfterTestExecutionCallback.afterTestExecution()} - Diagnostics captured here while NiFi is still running</li>
 *   <li>{@code @AfterEach} - NiFi instance is torn down here</li>
 *   <li>{@code TestWatcher.testFailed()} - Too late for live diagnostics</li>
 * </ol>
 */
public class TroubleshootingTestWatcher implements TestWatcher, AfterTestExecutionCallback {
    private static final Logger logger = LoggerFactory.getLogger(TroubleshootingTestWatcher.class);
    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(TroubleshootingTestWatcher.class);
    private static final String QUARANTINE_DIR_KEY = "quarantineDir";

    @Override
    public void afterTestExecution(final ExtensionContext context) {
        // Check if the test failed by looking for an execution exception
        final Optional<Throwable> executionException = context.getExecutionException();
        if (executionException.isEmpty()) {
            return; // Test passed, no need to capture diagnostics
        }

        final Optional<Object> optionalTestInstance = context.getTestInstance();
        if (optionalTestInstance.isEmpty()) {
            return;
        }

        final Object testInstance = optionalTestInstance.get();
        if (!(testInstance instanceof NiFiInstanceProvider provider)) {
            return;
        }

        final NiFiInstance instance = provider.getNiFiInstance();
        if (instance == null) {
            logger.warn("Test failed but NiFi Instance is not available for capturing diagnostics");
            return;
        }

        final String displayName = context.getDisplayName();
        final String testClassName = context.getTestClass().map(Class::getSimpleName).orElse("TestClassUnknown");
        final Throwable cause = executionException.get();

        try {
            final File quarantineDir = quarantineTroubleshootingInfo(instance, testClassName, displayName, cause);
            // Store the directory so testFailed() can log it
            context.getStore(NAMESPACE).put(QUARANTINE_DIR_KEY, quarantineDir);
            logger.info("Test Failed [{}]: Troubleshooting information (including live diagnostics) stored [{}]", displayName, quarantineDir.getAbsolutePath());
        } catch (final Exception e) {
            logger.error("Test Failed [{}]: Failed to capture troubleshooting information", displayName, e);
        }
    }

    @Override
    public void testFailed(final ExtensionContext context, final Throwable cause) {
        // Diagnostics have already been captured in afterTestExecution() while NiFi was still running.
        // This method is called after @AfterEach, so we just check if we already captured the info.
        final File quarantineDir = context.getStore(NAMESPACE).get(QUARANTINE_DIR_KEY, File.class);
        if (quarantineDir == null) {
            // Diagnostics weren't captured in afterTestExecution - try to capture what we can now
            final Optional<Object> optionalTestInstance = context.getTestInstance();
            if (optionalTestInstance.isPresent()) {
                final Object testInstance = optionalTestInstance.get();
                if (testInstance instanceof NiFiInstanceProvider provider) {
                    final String displayName = context.getDisplayName();
                    try {
                        final String testClassName = context.getTestClass().map(Class::getSimpleName).orElse("TestClassUnknown");
                        final File dir = quarantineTroubleshootingInfoFallback(provider, testClassName, displayName, cause);
                        logger.info("Test Failed [{}]: Troubleshooting information stored (without live diagnostics) [{}]", displayName, dir.getAbsolutePath());
                    } catch (final Exception e) {
                        logger.error("Test Failed [{}]: Troubleshooting information not stored", displayName, e);
                    }
                }
            }
        }
    }

    private File quarantineTroubleshootingInfo(final NiFiInstance instance, final String testClassName,
                                               final String methodName, final Throwable failureCause) throws IOException {
        final File troubleshooting = new File("target/troubleshooting");
        final String quarantineDirName = testClassName + "-" + methodName.replace("()", "");
        final File quarantineDir = new File(troubleshooting, quarantineDirName);
        quarantineDir.mkdirs();

        instance.quarantineTroubleshootingInfo(quarantineDir, failureCause);
        return quarantineDir;
    }

    private File quarantineTroubleshootingInfoFallback(final NiFiInstanceProvider provider, final String testClassName,
                                                       final String methodName, final Throwable failureCause) throws IOException {
        NiFiInstance instance = provider.getNiFiInstance();

        if (instance == null) {
            logger.warn("While capturing troubleshooting info for {}, the NiFi Instance is not available. Will create a new one for Diagnostics purposes, but live diagnostics will not be available " +
                "since it's not the same instance that ran the test", methodName);
            instance = provider.getInstanceFactory().createInstance();
        }

        final File troubleshooting = new File("target/troubleshooting");
        final String quarantineDirName = testClassName + "-" + methodName.replace("()", "");
        final File quarantineDir = new File(troubleshooting, quarantineDirName);
        quarantineDir.mkdirs();

        instance.quarantineTroubleshootingInfo(quarantineDir, failureCause);
        return quarantineDir;
    }
}
