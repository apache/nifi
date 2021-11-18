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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestHexDump {

    private String shortTestString =  "Appropriate test string";
    private String longTestString = "Lorem ipsum dolor sit amet, consectetur adipiscing"+
    " elit. Etiam eu nibh turpis. Etiam leo leo, rutrum vel sapien vitae, faucibus ferm"+
    "entum lorem. Proin sit amet augue eleifend, vulputate leo in, porta mauris. Phasel"+
    "lus eros arcu, scelerisque pulvinar neque quis, pretium ullamcorper sem. Sed at su"+
    "scipit odio. Cras lobortis orci arcu, ac vestibulum sem mollis at. Mauris mollis m"+
    "etus quis erat ultricies hendrerit. Aenean at justo vitae nisi dapibus egestas ege"+
    "t sed sem. Suspendisse mattis nunc risus, et consequat sem aliquet vel. Sed ut con"+
    "sectetur mi. Sed ut odio non ex pellentesque luctus ut at purus. Nunc tincidunt, t"+
    "ortor sit amet laoreet iaculis, leo ipsum porta ligula, ac lacinia libero orci id "+
    "nibh. Ut mattis sapien ac finibus malesuada.";

    private String specialTestString = "/^[!@#$%^&*()_+-=[]{};':|,.<>/?]*$/";

    private String longHexString = "4c6f72656d20697073756d20646f6c6f722073697420616d6"+
    "5742c20636f6e73656374657475722061646970697363696e6720656c69742e20457469616d206575"+
    "206e696268207475727069732e20457469616d206c656f206c656f2c2072757472756d2076656c207"+
    "3617069656e2076697461652c206661756369627573206665726d656e74756d206c6f72656d2e2050"+
    "726f696e2073697420616d657420617567756520656c656966656e642c2076756c707574617465206"+
    "c656f20696e2c20706f727461206d61757269732e2050686173656c6c75732065726f732061726375"+
    "2c207363656c657269737175652070756c76696e6172206e6571756520717569732c2070726574697"+
    "56d20756c6c616d636f727065722073656d2e20536564206174207375736369706974206f64696f2e"+
    "2043726173206c6f626f72746973206f72636920617263752c20616320766573746962756c756d207"+
    "3656d206d6f6c6c69732061742e204d6175726973206d6f6c6c6973206d6574757320717569732065"+
    "72617420756c747269636965732068656e6472657269742e2041656e65616e206174206a7573746f2"+
    "07669746165206e697369206461706962757320656765737461732065676574207365642073656d2e"+
    "2053757370656e6469737365206d6174746973206e756e632072697375732c20657420636f6e73657"+
    "17561742073656d20616c69717565742076656c2e2053656420757420636f6e736563746574757220"+
    "6d692e20536564207574206f64696f206e6f6e2065782070656c6c656e746573717565206c7563747"+
    "5732075742061742070757275732e204e756e632074696e636964756e742c20746f72746f72207369"+
    "7420616d6574206c616f7265657420696163756c69732c206c656f20697073756d20706f727461206"+
    "c6967756c612c206163206c6163696e6961206c696265726f206f726369206964206e6962682e2055"+
    "74206d61747469732073617069656e2061632066696e69627573206d616c6573756164612e";
    private String shortHexString = "417070726f707269617465207465737420737472696e67";
    private String specialHexString = "2f5e5b21402324255e262a28295f2b2d3d5b5d7b7d3b273a7c2c2e3c3e2f3f5d2a242f";


    private TestRunner createTestRunner(){
        /*
        Help function with purpose to minimize lines of code.
        Create TestRunner and returns runner reference.
        */
        return TestRunners.newTestRunner( new HexDump() );
    }

    private TestRunner executeTestRunner(TestRunner runner, String input, int Amount){
        /*
        Help function with purpose to minimize lines of code.
        Execute int amount of flowfiles with the input.
        Return runner reference.
        */

        InputStream content = new ByteArrayInputStream(input.getBytes());
        runner.enqueue(content);
        runner.run(Amount);
        runner.assertQueueEmpty();

        return runner;
    }

    private void validateFlowFileOutput(TestRunner runner, String expectedOutput){
        /*
        Help function with purpose to minimize lines of code.
        Compares incoming flowfile with expected output string.
        Return runner reference.
        */
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(HexDump.REL_SUCCESS);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        assertEquals(expectedOutput, resultValue);
    }

    private void validateNoFlowFileOutput(TestRunner runner){
        /*
        Help function with purpose to minimize lines of code.
        Return runner reference.
        */
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(HexDump.REL_SUCCESS);
        if (results.size() > 0){
            MockFlowFile result = results.get(0);
            String resultValue = new String(runner.getContentAsByteArray(result));
            assertEquals("", resultValue);
        } else {
            assertEquals(0, results.size());
        }
    }

    private void validateAttributeOutput(TestRunner runner, String expectedOutput){
        /*
        Help function with purpose to minimize lines of code.
        Compares incoming attribute data with expected output string.
        Return runner reference.
        */
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(HexDump.REL_SUCCESS);
        MockFlowFile result = results.get(0);
        result.assertAttributeExists(HexDump.ATTRIBUTE_NAME.getDefaultValue());
        result.assertAttributeEquals(HexDump.ATTRIBUTE_NAME.getDefaultValue(), expectedOutput);
    }

    private void validateNoAttributeOutput(TestRunner runner, String attributeName){
        /*
        Help function with purpose to minimize lines of code.
        Return runner reference.
        */
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(HexDump.REL_SUCCESS);
        MockFlowFile result = results.get(0);
        result.assertAttributeNotExists(attributeName);
    }

    private void convertionInputOutputTest(String input, String expectedOutput){
        /*
         Help function with purpose to minimize lines of code.
         Validation of hex convertion over different inputs and outputs.
        */
        TestRunner runner = createTestRunner();
        runner = executeTestRunner(runner, input, 1);
        validateFlowFileOutput(runner, expectedOutput);
        validateAttributeOutput(runner, expectedOutput.substring(0,16));
    }

    @Test
    public void testHexConvertionSpecialValuesValid() throws IOException  {
        convertionInputOutputTest(specialTestString, specialHexString);
    }

    @Test
    public void testHexConvertionShortValid() {
        /* Validate hex convertion results for message < 256 bytes */
        convertionInputOutputTest(shortTestString, shortHexString);
    }

    @Test
    public void testHexConvertionLongValid() {
         /* Validate hex convertion results for message > 256 bytes */
         convertionInputOutputTest(longTestString, longHexString);
    }

    @Test
    public void testModeFlowFileValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.OUT_MODE, "FLOWFILE");
        runner = executeTestRunner(runner, longTestString, 1);
        validateFlowFileOutput(runner, longHexString);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(HexDump.REL_SUCCESS);
        MockFlowFile result = results.get(0);
        result.assertAttributeNotExists(HexDump.ATTRIBUTE_NAME.getDefaultValue());
    }

    @Test
    public void testModeBothValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.OUT_MODE, "BOTH");
        runner = executeTestRunner(runner, longTestString, 1);
        validateFlowFileOutput(runner, longHexString);
        validateAttributeOutput(runner, longHexString.substring(0,16));
    }

    @Test
    public void testEmptyFlowFileValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.OUT_MODE, "BOTH");
        runner = executeTestRunner(runner, "", 1);
        validateFlowFileOutput(runner, "");
        validateAttributeOutput(runner, "");
    }

    @Test
    public void testModeAttributeValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.OUT_MODE, "ATTRIBUTE");
        runner = executeTestRunner(runner, longTestString, 1);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(HexDump.REL_SUCCESS);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        assertEquals("", resultValue);
        validateAttributeOutput(runner, longHexString.substring(0,16));
    }

    @Test
    public void testLimitedFlowFileLengthValid() {
        // see that size is 16 and equals corresponding 16 bytes from testString
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.FLOWFILE_LENGTH, "18");
        runner = executeTestRunner(runner, longTestString, 1);
        validateFlowFileOutput(runner, longHexString.substring(0,18));
    }

    @Test
    public void testFlowFileOffsetValid() {
        // see that offset is 16 and equals corresponding to testString
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.FLOWFILE_OFFSET, "18");
        runner = executeTestRunner(runner, longTestString, 1);
        validateFlowFileOutput(runner, longHexString.substring(18,longHexString.length()));
    }

    @Test
    public void testAttributeOffsetValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.ATTRIBUTE_OFFSET, "8");
        runner = executeTestRunner(runner, shortTestString, 1);
        validateAttributeOutput(runner, shortHexString.substring(8,24));
    }

    @Test
    public void testLimitedAttributeLengthValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.ATTRIBUTE_LENGTH, "8");
        runner = executeTestRunner(runner, shortTestString, 1);
        validateAttributeOutput(runner, shortHexString.substring(0,8));
    }

    @Test
    public void testAttributeNameChangeValid() {
        String testName =  "testNewAttributeName8";
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.ATTRIBUTE_NAME, testName);
        runner.setProperty(HexDump.ATTRIBUTE_LENGTH, "8");
        runner = executeTestRunner(runner, longTestString, 1);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(HexDump.REL_SUCCESS);
        MockFlowFile result = results.get(0);
        result.assertAttributeExists(testName);
    }

    @Test
    public void testLargeFlowFileLengthValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.FLOWFILE_LENGTH, "5000");
        runner = executeTestRunner(runner, longTestString, 1);
        validateFlowFileOutput(runner, longHexString);
    }

    @Test
    public void testOutOfBoundsFlowFileOffsetValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.FLOWFILE_LENGTH, "0");
        runner.setProperty(HexDump.FLOWFILE_OFFSET, "5000");
        runner = executeTestRunner(runner, shortHexString, 1);
        validateNoFlowFileOutput(runner);

        // Test offset and length limits
        runner = createTestRunner();
        runner.setProperty(HexDump.FLOWFILE_LENGTH, "255");
        runner.setProperty(HexDump.FLOWFILE_OFFSET, "10");
        runner = executeTestRunner(runner, shortTestString, 1);
        validateFlowFileOutput(runner, shortHexString.substring(10,shortHexString.length()));
    }

    @Test
    public void testTooLongAttributeLengthValid() {
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.ATTRIBUTE_LENGTH, "30000");
        runner = executeTestRunner(runner, longTestString, 1);
        validateAttributeOutput(runner, longHexString.substring(0,255));
    }

    @Test
    public void testOutOfBoundsAttributeOffsetValid() {
        // Test 255 > Attribute offset > incoming data
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.ATTRIBUTE_OFFSET, "200");
        runner = executeTestRunner(runner, shortTestString, 1);
        validateNoAttributeOutput(runner, HexDump.ATTRIBUTE_NAME.getDefaultValue());

        // Test offset and length limits
        runner = createTestRunner();
        runner.setProperty(HexDump.ATTRIBUTE_LENGTH, "255");
        runner.setProperty(HexDump.ATTRIBUTE_OFFSET, "8");
        runner = executeTestRunner(runner, shortTestString, 1);
        validateAttributeOutput(runner, shortHexString.substring(8,shortHexString.length()));
    }

    @Test
    public void testDefaultLargerQueueValid() {
        // Test queued capability of processor, without dropping content.
        int run_amount = 25;
        TestRunner runner = createTestRunner();
        runner.setProperty(HexDump.OUT_MODE, "BOTH");
        for (int i=1; i<=run_amount; i++){
            runner = executeTestRunner(runner, shortTestString, 1);
        }
        List<MockFlowFile> success_results = runner.getFlowFilesForRelationship(HexDump.REL_SUCCESS);
        assertEquals(run_amount, success_results.size());
    }

}
