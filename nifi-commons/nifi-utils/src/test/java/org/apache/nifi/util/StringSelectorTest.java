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
package org.apache.nifi.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringSelectorTest {
    @Test
    public void testNull() {
        test("", false, (String) null);
    }

    @Test
    public void testEmpty() {
        test("", false, "");
    }

    @Test
    public void testNull_Empty() {
        test("", false, null, "");
    }

    @Test
    public void testNull_Empty_NonEmpty() {
        test("expected", true, null, "", "expected");
    }

    @Test
    public void testNonEmpty_Null_NonEmpty() {
        test("expected", true, "expected", null, "other");
    }

    @Test
    public void testNonEmpty_Empty_NonEmpty() {
        test("expected", true, "expected", "", "other");
    }

    @Test
    public void testTwoNonEmpty() {
        test("expected", true, "expected", "other");
    }

    @Test
    public void testManyInputsWithNoExpected() {
        test(
            "",
            false,
            new String[]{null, "", "", ""},
            new String[]{null, null, ""},
            new String[]{null, "", null}
        );
    }

    @Test
    public void testManyInputsWithExpectedInFirstBatch() {
        test(
            "expected",
            true,
            new String[]{null, "expected", "", ""},
            new String[]{null, null, ""},
            new String[]{null, "other", "andAnother"}
        );
    }

    @Test
    public void testManyInputsWithExpectedInLaterBatch() {
        test(
            "expected",
            true,
            new String[]{null, "", "", ""},
            new String[]{null, null, "expected"},
            new String[]{null, "other", "andAnother"}
        );
    }

    public void test(String expected, boolean expectedFound, String... inputs) {
        // GIVEN

        // WHEN
        StringSelector selector = StringSelector.of(inputs);

        // THEN
        boolean actualFound = selector.found();
        String actual = selector.toString();

        assertEquals(expected, actual);
        assertEquals(expectedFound, actualFound);
    }

    public void test(String expected, boolean expectedFound, String[] firstInputs, String[]... otherInputs) {
        // GIVEN

        // WHEN
        StringSelector selector = StringSelector.of(firstInputs);
        for (String[] otherInput : otherInputs) {
            selector = selector.or(otherInput);

            if (selector.found()) {
                assertEquals(expected, selector.toString());
            } else {
                assertEquals("", selector.toString());
            }
        }

        // THEN
        boolean actualFound = selector.found();
        String actual = selector.toString();

        assertEquals(expected, actual);
        assertEquals(expectedFound, actualFound);

    }
}
