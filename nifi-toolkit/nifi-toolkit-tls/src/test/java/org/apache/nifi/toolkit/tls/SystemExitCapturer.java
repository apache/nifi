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

package org.apache.nifi.toolkit.tls;

import org.apache.nifi.toolkit.tls.commandLine.ExitCode;

import java.io.Closeable;
import java.io.IOException;
import java.security.Permission;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SystemExitCapturer implements Closeable {
    private final SecurityManager originalSecurityManager;

    public SystemExitCapturer() {
        originalSecurityManager = System.getSecurityManager();
        // [see http://stackoverflow.com/questions/309396/java-how-to-test-methods-that-call-system-exit#answer-309427]
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {
                // Noop
            }

            @Override
            public void checkPermission(Permission perm, Object context) {
                // Noop
            }

            @Override
            public void checkExit(int status) {
                super.checkExit(status);
                throw new ExitException(status);
            }
        });
    }

    public void runAndAssertExitCode(Runnable runnable, ExitCode exitCode) {
        try {
            runnable.run();
            fail("Expecting exit code " + exitCode);
        } catch (ExitException e) {
            assertEquals("Expecting exit code: " + exitCode + ", got " + ExitCode.values()[e.getExitCode()], exitCode.ordinal(), e.getExitCode());
        }
    }

    @Override
    public void close() throws IOException {
        System.setSecurityManager(originalSecurityManager);
    }
}
