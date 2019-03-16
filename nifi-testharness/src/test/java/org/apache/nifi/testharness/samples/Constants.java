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


package org.apache.nifi.testharness.samples;

import java.io.File;

public final class Constants {

    static final File OUTPUT_DIR = new File("./NiFiTest/NiFiReadTest");

    // NOTE: you will have to have the NiFi distribution ZIP placed into this directory.
    // Its version must be the same as the one referenced in the flow.xml, otherwise it will not work!
    static final File NIFI_ZIP_DIR = new File("../../nifi-assembly/target");

    static final File FLOW_XML_FILE = new File(NiFiMockFlowTest.class.getResource("/flow.xml").getFile());
}
