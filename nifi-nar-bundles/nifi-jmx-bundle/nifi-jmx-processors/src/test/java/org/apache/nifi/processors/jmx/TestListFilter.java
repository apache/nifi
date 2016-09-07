package org.apache.nifi.processors.jmx;

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

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestListFilter {
    @Test
    public void validateListDomain1() throws Exception {
        ListFilter listFilter = new ListFilter("domain1");

        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type1"));
        assertFalse(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type1"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type1"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain2", "type1"));
    }

    @Test
    public void validateListDomain2() throws Exception {
        ListFilter listFilter = new ListFilter("domain1,domain2");

        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type1"));
        assertFalse(listFilter.isInList(ListFilter.WHITELIST, "domain3", "type1"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type1"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain2", "type1"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain3", "type1"));
    }

    @Test
    public void validateListDomain1Type1() throws Exception {
        ListFilter listFilter = new ListFilter("domain1:type1");

        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type1"));
        assertFalse(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type2"));
        assertFalse(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type1"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type1"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type2"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain2", "type1"));
    }

    @Test
    public void validateListDomain1Type2() throws Exception {
        ListFilter listFilter = new ListFilter("domain1:type1 type2");

        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type2"));
        assertFalse(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type3"));
        assertFalse(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type1"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type1"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type2"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type3"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain2", "type1"));
    }

    @Test
    public void validateListDomain2Type2() throws Exception {
        ListFilter listFilter = new ListFilter("domain1:type1 type2,domain2:type1 type2");

        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type2"));
        assertFalse(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type3"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type2"));
        assertFalse(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type3"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type1"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type2"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type3"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain2", "type1"));
        assertTrue(listFilter.isInList(ListFilter.BLACKLIST, "domain2", "type2"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain2", "type3"));
    }

    @Test
    public void validateListDomainRegex() throws Exception {
        ListFilter listFilter = new ListFilter("dom.*");

        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "dom", "type1"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "xdomain1", "type1"));
    }

    @Test
    public void validateListDomainRegexTypeRegex() throws Exception {
        ListFilter listFilter = new ListFilter("dom.*:t.*");

        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type2"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain2", "type2"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "dom", "type1"));
        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "dom", "t"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "xdomain1", "type1"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "xtype1"));
    }

    @Test
    public void validateWhiteBlackListDomain1() throws Exception {
        ListFilter listFilter = new ListFilter("");

        assertTrue(listFilter.isInList(ListFilter.WHITELIST, "domain1", "type1"));
        assertFalse(listFilter.isInList(ListFilter.BLACKLIST, "domain1", "type1"));
    }
}
