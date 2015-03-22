/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.solr;

import org.junit.Assert;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.junit.Test;


public class RequestParamsUtilTest {

    @Test
    public void testSimpleParse() {
        MultiMapSolrParams map = RequestParamsUtil.parse("a=1&b=2&c=3");
        Assert.assertEquals("1", map.get("a"));
        Assert.assertEquals("2", map.get("b"));
        Assert.assertEquals("3", map.get("c"));
    }

    @Test
    public void testParseWithSpaces() {
        MultiMapSolrParams map = RequestParamsUtil.parse("a = 1 &b= 2& c= 3 ");
        Assert.assertEquals("1", map.get("a"));
        Assert.assertEquals("2", map.get("b"));
        Assert.assertEquals("3", map.get("c"));
    }

    @Test(expected = IllegalStateException.class)
    public void testMalformedParamsParse() {
        RequestParamsUtil.parse("a=1&b&c=3");
    }

}
