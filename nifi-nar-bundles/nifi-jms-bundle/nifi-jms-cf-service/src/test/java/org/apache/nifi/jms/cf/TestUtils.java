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
package org.apache.nifi.jms.cf;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
class TestUtils {

    static Logger logger = LoggerFactory.getLogger(TestUtils.class);

    static String setupActiveMqLibForTesting(boolean clean) {
        String[] urlsStrings = new String[]{
                "http://central.maven.org/maven2/org/apache/activemq/activemq-client/5.13.0/activemq-client-5.13.0.jar",
                "http://central.maven.org/maven2/org/apache/activemq/activemq-broker/5.13.0/activemq-broker-5.13.0.jar",
                "http://central.maven.org/maven2/org/apache/geronimo/specs/geronimo-j2ee-management_1.0_spec/1.0.1/geronimo-j2ee-management_1.0_spec-1.0.1.jar",
                "http://central.maven.org/maven2/org/fusesource/hawtbuf/hawtbuf/1.11/hawtbuf-1.11.jar" };

        try {
            File activeMqLib = new File("target/active-mq-lib");
            if (activeMqLib.exists() && clean) {
                FileUtils.deleteDirectory(activeMqLib);
            }
            activeMqLib.mkdirs();
            for (String urlString : urlsStrings) {
                URL url = new URL(urlString);
                String path = url.getPath();
                path = path.substring(path.lastIndexOf("/") + 1);
                logger.info("Downloading: " + path);
                ReadableByteChannel rbc = Channels.newChannel(url.openStream());
                try (FileOutputStream fos = new FileOutputStream(new File(activeMqLib, path))) {
                    fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                    fos.close();
                }
            }
            return activeMqLib.getAbsolutePath();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to download ActiveMQ libraries.", e);
        }
    }
}
