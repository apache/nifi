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
package org.apache.nifi.processors.standard;

import org.apache.nifi.processors.standard.util.TestPutTCPCommon;

public class TestPutTCP extends TestPutTCPCommon {

    public TestPutTCP() {
        super();
        ssl = false;
    }

    @Override
    public void configureProperties(String host, int port, String outgoingMessageDelimiter, boolean connectionPerFlowFile, boolean expectValid) {
        runner.setProperty(PutTCP.HOSTNAME, host);
        runner.setProperty(PutTCP.PORT, Integer.toString(port));
        if (outgoingMessageDelimiter != null) {
            runner.setProperty(PutTCP.OUTGOING_MESSAGE_DELIMITER, outgoingMessageDelimiter);
        }

        runner.setProperty(PutTCP.CONNECTION_PER_FLOWFILE, String.valueOf(connectionPerFlowFile));

        if (expectValid) {
            runner.assertValid();
        } else {
            runner.assertNotValid();
        }
    }


}
