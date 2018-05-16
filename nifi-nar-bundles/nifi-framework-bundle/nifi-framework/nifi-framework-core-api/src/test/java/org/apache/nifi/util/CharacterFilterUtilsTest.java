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

import org.junit.Assert;
import org.junit.Test;

public class CharacterFilterUtilsTest {

    @Test
    public void filterInvalidCharacters() throws Exception {
        final String text = "This is an example with characters that need to be filtered \u0002 in it. " + Character.MIN_SURROGATE;
        final String filtered = CharacterFilterUtils.filterInvalidXmlCharacters(text);

        final String expected = "This is an example with characters that need to be filtered  in it. ";
        Assert.assertEquals(expected, filtered);
    }

}