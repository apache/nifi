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
package org.apache.nifi.web.search.attributematchers;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AttributeMatcherTest {
    private final static String SEARCH_TERM = "lorem";
    private final static String SUBJECT_PARTIAL = SEARCH_TERM;
    private final static String SUBJECT_PARTIAL_UPPERCASE = SEARCH_TERM.toUpperCase();
    private final static String SUBJECT_FULL = SUBJECT_PARTIAL + " ipsum";
    private final static String LABEL = "label";
    private final static String LABEL_2 = "label2";

    @Test
    public void testWhenEqualsThenAdded() {
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_PARTIAL, LABEL, matches);

        // then
        Assert.assertEquals(1, matches.size());
        Assert.assertTrue(matches.contains(LABEL + AttributeMatcher.SEPARATOR + SUBJECT_PARTIAL));
    }

    @Test
    public void testWhenSubstringThenAdded() {
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_FULL, LABEL, matches);

        // then
        Assert.assertEquals(1, matches.size());
        Assert.assertTrue(matches.contains(LABEL + AttributeMatcher.SEPARATOR + SUBJECT_FULL));
    }

    @Test
    public void testWhenOnlyCaseDifferenceThenAdded() {
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_PARTIAL_UPPERCASE, LABEL, matches);

        // then
        Assert.assertEquals(1, matches.size());
        Assert.assertTrue(matches.contains(LABEL + AttributeMatcher.SEPARATOR + SUBJECT_PARTIAL_UPPERCASE));
    }

    @Test
    public void testWhenNonMatchingThenNotAdded() {
        // given
        final String nonMatchingSubject = "foobar";
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, nonMatchingSubject, LABEL, matches);

        // then
        Assert.assertEquals(0, matches.size());
    }

    @Test
    public void testWhenSubjectIsNullThenNotAdded() {
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, null, LABEL, matches);

        // then
        Assert.assertEquals(0, matches.size());
    }

    @Test
    public void testWhenSearchTermIsNullThenNotAdded() {
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(null, SUBJECT_PARTIAL, LABEL, matches);

        // then
        Assert.assertEquals(0, matches.size());
    }

    @Test
    public void testWhenSearchTermAndSubjectAreNullThenNotAdded() {
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(null, null, LABEL, matches);

        // then
        Assert.assertEquals(0, matches.size());
    }

    @Test
    public void testWhenMatchesIsNullThenNoException() {
        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_PARTIAL, LABEL, null);

        // then - no exception
    }

    @Test
    public void testWhenLabelIsNullThenSkipped() {
        // Test to cover backward compatibility
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_PARTIAL, null, matches);

        // then
        Assert.assertEquals(1, matches.size());
        Assert.assertTrue(matches.contains("null: " + SUBJECT_PARTIAL));
    }

    @Test
    public void testWhenAddingMultipleThenOrderIsPreserved() {
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_PARTIAL, LABEL, matches);
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_FULL, LABEL_2, matches);

        // then
        Assert.assertEquals(2, matches.size());
        Assert.assertEquals(LABEL + AttributeMatcher.SEPARATOR + SUBJECT_PARTIAL, matches.get(0));
        Assert.assertEquals(LABEL_2 + AttributeMatcher.SEPARATOR + SUBJECT_FULL, matches.get(1));
    }

    @Test
    public void testWhenDuplicatedThenNotAddedTwice() {
        // given
        final List<String> matches = new ArrayList<>();

        // when
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_PARTIAL, LABEL, matches);
        AttributeMatcher.addIfMatching(SEARCH_TERM, SUBJECT_PARTIAL, LABEL, matches);

        // then
        Assert.assertEquals(1, matches.size());
        Assert.assertTrue(matches.contains(LABEL + AttributeMatcher.SEPARATOR + SUBJECT_PARTIAL));
    }
}