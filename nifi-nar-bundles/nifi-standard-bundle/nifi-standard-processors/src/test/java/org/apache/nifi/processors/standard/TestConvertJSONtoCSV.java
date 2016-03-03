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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class TestConvertJSONtoCSV {

    @Test
    public void testSimpleJSON() throws IOException {

        final TestRunner runner = TestRunners.newTestRunner(new ConvertJSONtoCSV());
        runner.setProperty(ConvertJSONtoCSV.DELIMITER, "|");
        runner.setProperty(ConvertJSONtoCSV.EMPTY_FIELDS, "NULL");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONtoCSV/simple.json"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertJSONtoCSV.RELATIONSHIP_SUCCESS);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONtoCSV.RELATIONSHIP_SUCCESS).get(0);
        out.assertContentEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestConvertJSONtoCSV/simple.csv"))));
    }

    @Test
    public void testTweetMultiNestedJSON() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertJSONtoCSV());
        runner.setProperty(ConvertJSONtoCSV.DELIMITER, ",");
        runner.setProperty(ConvertJSONtoCSV.REMOVE_FIELDS, "retweeted_status,entities");

        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONtoCSV/tweet.json"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertJSONtoCSV.RELATIONSHIP_SUCCESS);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONtoCSV.RELATIONSHIP_SUCCESS).get(0);
        out.assertContentEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestConvertJSONtoCSV/tweet.csv"))));
    }

    @Test
    public void testMalformedJSON() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertJSONtoCSV());
        runner.setProperty(ConvertJSONtoCSV.DELIMITER, ",");
        runner.setProperty(ConvertJSONtoCSV.REMOVE_FIELDS, "NULL");

        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONtoCSV/simpleMalformed.json"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertJSONtoCSV.RELATIONSHIP_FAILURE);
    }

    @Test
    public void testNonJSON() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertJSONtoCSV());
        runner.setProperty(ConvertJSONtoCSV.DELIMITER, ",");

        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONtoCSV/simple.csv"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertJSONtoCSV.RELATIONSHIP_FAILURE);
    }

    @Test
    public void testJSONObject() throws IOException{
        final TestRunner runner = TestRunners.newTestRunner(new ConvertJSONtoCSV());
        runner.setProperty(ConvertJSONtoCSV.DELIMITER, ",");

        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONtoCSV/object.json"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertJSONtoCSV.RELATIONSHIP_SUCCESS);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONtoCSV.RELATIONSHIP_SUCCESS).get(0);
        out.assertContentEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestConvertJSONtoCSV/object.csv"))));
    }

    @Test
    public void testJSONObjectNoHeadeRemoveFields() throws IOException{
        final TestRunner runner = TestRunners.newTestRunner(new ConvertJSONtoCSV());
        runner.setProperty(ConvertJSONtoCSV.DELIMITER, "^");
        runner.setProperty(ConvertJSONtoCSV.REMOVE_FIELDS,"price");
        runner.setProperty(ConvertJSONtoCSV.INCLUDE_HEADERS,"False");

        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONtoCSV/object.json"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertJSONtoCSV.RELATIONSHIP_SUCCESS);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONtoCSV.RELATIONSHIP_SUCCESS).get(0);
        out.assertContentEquals("1^A green door^home^green" + "\n");
    }
}
