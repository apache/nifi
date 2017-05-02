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
package org.apache.nifi.hl7.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.hl7.hapi.HapiMessage;
import org.apache.nifi.hl7.model.HL7Field;
import org.apache.nifi.hl7.model.HL7Message;
import org.junit.Before;
import org.junit.Test;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.validation.impl.ValidationContextFactory;

@SuppressWarnings("resource")
public class TestHL7Query {

    public static final String HYPERGLYCEMIA =
        "MSH|^~\\&|XXXXXX||HealthOrg01||||ORU^R01|Q1111111111111111111|P|2.3|\r\n" +
        "PID|||000000001||SMITH^JOHN||19700101|M||||||||||999999999999|123456789|\r\n" +
        "PD1||||1234567890^LAST^FIRST^M^^^^^NPI|\r\n" +
        "OBR|1|341856649^HNAM_ORDERID|000000000000000000|648088^Basic Metabolic Panel|||20150101000100|||||||||1620^Johnson^John^R||||||20150101000100|||M|||||||||||20150101000100|\r\n" +
        "OBX|1|NM|GLU^Glucose Lvl|159|mg/dL|65-99^65^99|H|||F|||20150101000100|";

    public static final String HYPOGLYCEMIA =
        "MSH|^~\\&|XXXXXX||HealthOrg01||||ORU^R01|Q1111111111111111111|P|2.3|\r\n" +
        "PID|||000000001||SMITH^JOHN||19700101|M||||||||||999999999999|123456789|\r\n" +
        "PD1||||1234567890^LAST^FIRST^M^^^^^NPI|\r\n" +
        "OBR|1|341856649^HNAM_ORDERID|000000000000000000|648088^Basic Metabolic Panel|||20150101000100|||||||||1620^Johnson^John^R||||||20150101000100|||M|||||||||||20150101000100|\r\n" +
        "OBX|1|NM|GLU^Glucose Lvl|59|mg/dL|65-99^65^99|L|||F|||20150101000100|";

    private HL7Message hyperglycemia;

    private HL7Message hypoglycemia;

    @Before
    public void init() throws IOException, HL7Exception {
        this.hyperglycemia = createMessage(HYPERGLYCEMIA);
        this.hypoglycemia = createMessage(HYPOGLYCEMIA);
    }

    @Test
    public void testAssignAliases() {
        final LinkedHashMap<String, List<Object>> possibleValueMap = new LinkedHashMap<>();

        final List<Object> valuesA = new ArrayList<>();
        valuesA.add("a");
        valuesA.add("b");
        valuesA.add("c");

        final List<Object> valuesB = new ArrayList<>();
        valuesB.add("d");

        final List<Object> valuesC = new ArrayList<>();
        valuesC.add("e");
        valuesC.add("f");

        final List<Object> valuesD = new ArrayList<>();
        valuesD.add("g");
        valuesD.add("h");

        possibleValueMap.put("A", valuesA);
        possibleValueMap.put("B", valuesB);
        possibleValueMap.put("C", valuesC);
        possibleValueMap.put("D", valuesD);

        for (int i = 0; i < valuesA.size() * valuesB.size() * valuesC.size() * valuesD.size(); i++) {
            System.out.println(i + " : " + HL7Query.assignAliases(possibleValueMap, i));
        }

        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 0), "a", "d", "e", "g");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 1), "b", "d", "e", "g");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 2), "c", "d", "e", "g");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 3), "a", "d", "f", "g");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 4), "b", "d", "f", "g");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 5), "c", "d", "f", "g");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 6), "a", "d", "e", "h");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 7), "b", "d", "e", "h");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 8), "c", "d", "e", "h");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 9), "a", "d", "f", "h");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 10), "b", "d", "f", "h");
        verifyAssignments(HL7Query.assignAliases(possibleValueMap, 11), "c", "d", "f", "h");
    }

    private void verifyAssignments(final Map<String, Object> map, final String a, final String b, final String c, final String d) {
        assertEquals(a, map.get("A"));
        assertEquals(b, map.get("B"));
        assertEquals(c, map.get("C"));
        assertEquals(d, map.get("D"));
    }

    @Test
    public void testSelectMessage() throws HL7Exception, IOException {
        final HL7Query query = HL7Query.compile("SELECT MESSAGE");
        final HL7Message msg = hypoglycemia;
        final QueryResult result = query.evaluate(msg);
        assertTrue(result.isMatch());
        final List<String> labels = result.getLabels();
        assertEquals(1, labels.size());
        assertEquals("MESSAGE", labels.get(0));

        assertEquals(1, result.getHitCount());
        assertEquals(msg, result.nextHit().getValue("MESSAGE"));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testSelectField() throws HL7Exception, IOException {
        final HL7Query query = HL7Query.compile("SELECT PID.5");
        final HL7Message msg = hypoglycemia;
        final QueryResult result = query.evaluate(msg);
        assertTrue(result.isMatch());
        final List<String> labels = result.getLabels();
        assertEquals(1, labels.size());
        assertEquals(1, result.getHitCount());

        final Object names = result.nextHit().getValue("PID.5");
        assertTrue(names instanceof List);
        final List<Object> nameList = (List) names;
        assertEquals(1, nameList.size());
        final HL7Field nameField = (HL7Field) nameList.get(0);
        assertEquals("SMITH^JOHN", nameField.getValue());
    }

    @Test
    public void testSelectAbnormalTestResult() throws HL7Exception, IOException {
        final String query = "DECLARE result AS REQUIRED OBX SELECT result WHERE result.7 != 'N' AND result.1 = 1";

        final HL7Query hl7Query = HL7Query.compile(query);
        final QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());
    }

    @Test
    public void testFieldEqualsString() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.7 = 'L'");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.7 = 'H'");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());
    }

    @Test
    public void testLessThan() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 < 600");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 < 59");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());
    }

    @Test
    public void testCompareTwoFields() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 < result.6.2");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE NOT(result.4 > result.6.3)");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());
    }

    @Test
    public void testLessThanOrEqual() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 <= 59");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 <= 600");
        result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 <= 58");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());
    }

    @Test
    public void testGreaterThanOrEqual() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 >= 59");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 >= 6");
        result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 >= 580");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());
    }

    @Test
    public void testGreaterThan() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 > 58");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 > 6");
        result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.4 > 580");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());
    }

    @Test
    public void testDistinctValuesReturned() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result1 AS REQUIRED OBX, result2 AS REQUIRED OBX SELECT MESSAGE WHERE result1.7 = 'L' OR result2.7 != 'H'");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());
        assertEquals(1, result.getHitCount());
    }

    @Test
    public void testAndWithParents() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.7 = 'L' AND result.3.1 = 'GLU'");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.7 = 'L' AND result.3.1 = 'GLU'");
        result = hl7Query.evaluate(hyperglycemia);
        assertFalse(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.7 = 'H' AND result.3.1 = 'GLU'");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.7 = 'H' AND result.3.1 = 'GLU'");
        result = hl7Query.evaluate(hyperglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE (result.7 = 'H') AND (result.3.1 = 'GLU')");
        result = hl7Query.evaluate(hyperglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE ((result.7 = 'H') AND (result.3.1 = 'GLU'))");
        result = hl7Query.evaluate(hyperglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE (( ((result.7 = 'H')) AND ( ((result.3.1 = 'GLU')) )))");
        result = hl7Query.evaluate(hyperglycemia);
        assertTrue(result.isMatch());

    }

    @Test
    public void testIsNull() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.999 IS NULL");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.1 IS NULL");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());

        hl7Query = HL7Query.compile("SELECT MESSAGE WHERE ZZZ IS NULL");
        result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("SELECT MESSAGE WHERE OBX IS NULL");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());
    }

    @Test
    public void testNotNull() throws HL7Exception, IOException {
        HL7Query hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.999 NOT NULL");
        QueryResult result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());

        hl7Query = HL7Query.compile("DECLARE result AS REQUIRED OBX SELECT MESSAGE WHERE result.1 NOT NULL");
        result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());

        hl7Query = HL7Query.compile("SELECT MESSAGE WHERE ZZZ NOT NULL");
        result = hl7Query.evaluate(hypoglycemia);
        assertFalse(result.isMatch());

        hl7Query = HL7Query.compile("SELECT MESSAGE WHERE OBX NOT NULL");
        result = hl7Query.evaluate(hypoglycemia);
        assertTrue(result.isMatch());
    }

    private HL7Message createMessage(final String msgText) throws HL7Exception, IOException {
        final HapiContext hapiContext = new DefaultHapiContext();
        hapiContext.setValidationContext(ValidationContextFactory.noValidation());

        final PipeParser parser = hapiContext.getPipeParser();
        final Message message = parser.parse(msgText);
        return new HapiMessage(message);
    }

}
