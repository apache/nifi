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

package org.apache.nifi.minifi.bootstrap.util;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertTrue;

public class TestConfigTransformer {

    @Test
    public void doesTransformFile() throws Exception {

        ConfigTransformer.transformConfigFile("./src/test/resources/config.yml", "./target/");
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformInputStream() throws Exception {
        File inputFile = new File("./src/test/resources/config.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/");

        File nifiPropertiesFile = new File("./target/nifi.properties");
        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }
}
