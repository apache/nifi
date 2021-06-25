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
package org.apache.nifi.bootstrap.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OSUtilsTest {

    @Test
    public void testGetPid() throws IOException {
        final ProcessBuilder builder = new ProcessBuilder();
        final Process process = builder.command("java").start();
        final Logger logger = LoggerFactory.getLogger("testing");
        final Long pid = OSUtils.getProcessId(process, logger);
        process.destroy();
        assertNotNull("Process ID not found", pid);
    }

    @Test
    public void testParseJavaVersion8() {
        final String[] versions = new String[] { "1.8", "1.8.0", "1.8.0_100" };
        for (final String version : versions) {
            assertEquals(8, OSUtils.parseJavaVersion(version));
        }
    }

    @Test
    public void testParseJavaVersion11() {
        final String[] versions = new String[] { "11", "11.0", "11.0.11" };
        for (final String version : versions) {
            assertEquals(11, OSUtils.parseJavaVersion(version));
        }
    }
}
