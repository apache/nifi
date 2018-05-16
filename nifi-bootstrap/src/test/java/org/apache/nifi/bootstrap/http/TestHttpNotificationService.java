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
package org.apache.nifi.bootstrap.http;

import okhttp3.mockwebserver.MockWebServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.internal.util.io.IOUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestHttpNotificationService extends  TestHttpNotificationServiceCommon{


    static final String CONFIGURATION_FILE_TEXT = "\n"+
            "<services>\n"+
            "         <service>\n"+
            "            <id>http-notification</id>\n"+
            "            <class>org.apache.nifi.bootstrap.notification.http.HttpNotificationService</class>\n"+
            "            <property name=\"URL\">${test.server}</property>\n"+
            "            <property name=\"testProp\">${literal('testing')}</property>\n"+
            "         </service>\n"+
            "</services>";


    @BeforeClass
    public static void startServer() throws IOException {
        tempConfigFilePath = "./target/TestHttpNotificationService-config.xml";

        Files.deleteIfExists(Paths.get(tempConfigFilePath));

        mockWebServer = new MockWebServer();

        String configFileOutput = CONFIGURATION_FILE_TEXT.replace("${test.server}", String.valueOf(mockWebServer.url("/")));
        IOUtil.writeText(configFileOutput, new File(tempConfigFilePath));
    }

    @AfterClass
    public static void shutdownServer() throws IOException {
        Files.deleteIfExists(Paths.get(tempConfigFilePath));
        mockWebServer.shutdown();
    }

}
