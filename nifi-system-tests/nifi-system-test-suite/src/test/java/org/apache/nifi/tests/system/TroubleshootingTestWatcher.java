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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * Test Watcher responsible for writing troubleshooting logs
 */
public class TroubleshootingTestWatcher implements TestWatcher {
    private static final Logger logger = LoggerFactory.getLogger(TroubleshootingTestWatcher.class);

    @Override
    public void testFailed(final ExtensionContext context, final Throwable cause) {
        final Optional<Object> optionalTestInstance = context.getTestInstance();
        if (optionalTestInstance.isPresent()) {
            final Object testInstance = optionalTestInstance.get();
            if (testInstance instanceof NiFiInstanceProvider) {
                final NiFiInstanceProvider provider = (NiFiInstanceProvider) testInstance;
                final String displayName = context.getDisplayName();
                try {
                    final String testClassName = context.getTestClass().map(Class::getSimpleName).orElse("TestClassUnknown");
                    final File dir = quarantineTroubleshootingInfo(provider, testClassName, displayName, cause);
                    logger.info("Test Failed [{}]: Troubleshooting information stored [{}]", displayName, dir.getAbsolutePath());
                } catch (final Exception e) {
                    logger.error("Test Failed [{}]: Troubleshooting information not stored", displayName, e);
                }
            }
        }
    }

    private File quarantineTroubleshootingInfo(final NiFiInstanceProvider provider, final String testClassName, final String methodName, final Throwable failureCause) throws IOException {
        NiFiInstance instance = provider.getNiFiInstance();

        // The teardown method may or may not have already run at this point. If it has, the instance will be null.
        // In that case, just create a new instance and use it - it will map to the same directories.
        if (instance == null) {
            logger.warn("While capturing troubleshooting info for {}, the NiFi Instance is not available. Will create a new one for Diagnostics purposes, but some of the diagnostics may be less " +
                "accurate, since it's not the same instance that ran the test", methodName);

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
