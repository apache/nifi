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
package org.apache.nifi.processors.validator;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class NiFiJsonValidatorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(NiFiJsonValidator.class);
    }

    @Test
    public void testJsonValidatorSuccess() {

        String jsonSchema = "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"properties\": {\n" +
                "    \"name\": {\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"age\": {\n" +
                "      \"type\": \"integer\"\n" +
                "    },\n" +
                "    \"weight\": {\n" +
                "      \"type\": \"integer\"\n" +
                "    },\n" +
                "    \"title\": {\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\n" +
                "    \"name\",\n" +
                "    \"age\",\n" +
                "    \"weight\",\n" +
                "    \"title\"\n" +
                "  ]\n" +
                "}";

        testRunner.setProperty( NiFiJsonValidator.JSON_SCHEMA ,jsonSchema );
        testRunner.setIncomingConnection(true);

        String content1 = "{\n" +
                "    \"name\": \"Steven\",\n" +
                "    \"age\": 20,\n" +
                "    \"weight\": 100,\n" +
                "    \"title\": \"Engineer\"\n" +
                "}";
        System.out.println("flow file content");

        String content2 = "{\n" +
                "    \"name\": \"Jason\",\n" +
                "    \"age\": 22,\n" +
                "    \"weight\": 110,\n" +
                "    \"title\": \"Teacher\"\n" +
                "}";

        testRunner.enqueue( content1 );
        testRunner.enqueue( content2 );
        testRunner.run(2);
        testRunner.assertAllFlowFilesTransferred( NiFiJsonValidator.REL_SUCCESS , 2);

    }

    @Test
    public void testJsonValidatorFailed() {

        String jsonSchema = "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"properties\": {\n" +
                "    \"name\": {\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"age\": {\n" +
                "      \"type\": \"integer\"\n" +
                "    },\n" +
                "    \"weight\": {\n" +
                "      \"type\": \"integer\"\n" +
                "    },\n" +
                "    \"title\": {\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\n" +
                "    \"name\",\n" +
                "    \"age\",\n" +
                "    \"weight\",\n" +
                "    \"title\"\n" +
                "  ]\n" +
                "}";


        testRunner.setProperty( NiFiJsonValidator.JSON_SCHEMA ,jsonSchema );
        testRunner.setIncomingConnection(true);

        String content1 = "{\n" +
                "    \"name\": \"Steven\",\n" +
                "    \"age\": 20,\n" +
                "    \"weight\": 100,\n" +
                "    \"title\": \"Engineer\"\n" +
                "}";

        String content2 = "{\n" +
                "    \"name\": \"Jason\",\n" +
                "    \"weight\": 110,\n" +
                "    \"title\": \"Teacher\"\n" +
                "}";

        testRunner.enqueue( content1 );
        testRunner.enqueue( content2 );
        testRunner.run(2);

        testRunner.assertTransferCount(NiFiJsonValidator.REL_SUCCESS , 1);
        testRunner.assertTransferCount(NiFiJsonValidator.REL_FAILURE , 1);

    }

}
