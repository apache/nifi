/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.record.path;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRecordFieldRemover {
    @ParameterizedTest
    @ValueSource(strings = {"[*]", "[ * ]", "[* ]", "[ *]", "/street[*]", "//[*]", //
            "[0..-1]", "[ 0..-1]", "[0..-1 ]", "[0 ..-1]", "[0.. -1]", "[ 0 .. -1 ]"})
    void testIsAppliedToAllElementsInCollection(final String input) {
        assertTrue(new RecordFieldRemover.RecordPathRemovalProperties("/addresses" + input)
                .isAppliedToAllElementsInCollection());
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "", "/", "//", "/street", "[*]/street", "[1]", "[ -1 ]", "[0, 1]", "['one']"})
    void testNotIsAppliedToAllElementsInCollection(final String input) {
        assertFalse(new RecordFieldRemover.RecordPathRemovalProperties("/addresses" + input)
                .isAppliedToAllElementsInCollection());
    }

    @ParameterizedTest
    @ValueSource(strings = {"[1]", "[-1]", "[1]/street", "/street[ 1, 2 ]", "//[ -1,-2,3]"})
    void testIsAppliedToIndividualArrayElements(final String input) {
        assertTrue(new RecordFieldRemover.RecordPathRemovalProperties("/addresses" + input)
                .isAppliedToIndividualArrayElements());
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "", "/", "//", "/street", "[*]", "[ 0..-1 ]", "['one']"})
    void testNotIsAppliedToIndividualArrayElements(final String input) {
        assertFalse(new RecordFieldRemover.RecordPathRemovalProperties("/addresses" + input)
                .isAppliedToIndividualArrayElements());
    }

    @ParameterizedTest
    @ValueSource(strings = {"['one']", "[ 'one', 'two' ]", "['one']/street", "/street[ 'one, two' ]", "//['one' , 'two']"})
    void testIsAppliedToIndividualMapElements(final String input) {
        assertTrue(new RecordFieldRemover.RecordPathRemovalProperties("/addresses" + input)
                .isAppliedToIndividualMapElements());
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "", "/", "//", "/street", "[1]", "[ -1 ]", "[0, 1]", "[*]", "[ 0..-1 ]"})
    void testNotIsAppliedToIndividualMapElements(final String input) {
        assertFalse(new RecordFieldRemover.RecordPathRemovalProperties("/addresses" + input)
                .isAppliedToIndividualMapElements());
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "", "/", "//", "/street", "[*]", "[ 0..-1 ]"})
    void testIsPathRemovalRequiresSchemaModification(final String input) {
        assertTrue(new RecordFieldRemover.RecordPathRemovalProperties("/addresses" + input)
                .isRemovingFieldsNotJustElementsFromWithinCollection());
    }

    @ParameterizedTest
    @ValueSource(strings = {"[1]", "[-1]", "/street[ 1, 2 ]", "//[ -1,-2,3]", //
            "['one']", "[ 'one', 'two' ]", "/street[ 'one, two' ]", "//['one' , 'two']"})
    void testNotIsPathRemovalRequiresSchemaModification(final String input) {
        assertFalse(new RecordFieldRemover.RecordPathRemovalProperties("/addresses" + input)
                .isRemovingFieldsNotJustElementsFromWithinCollection());
    }
}
