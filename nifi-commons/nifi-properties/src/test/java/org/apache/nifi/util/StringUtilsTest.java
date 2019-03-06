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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import org.junit.Test;

public class StringUtilsTest {

  @Test
  public void testIsBlank() {
    assertFalse(StringUtils.isBlank("0"));
    assertFalse(StringUtils.isBlank("\u0000"));
    assertFalse(StringUtils.isBlank(" \u0000 "));
    assertFalse(StringUtils.isBlank(" \u5678 "));

    assertTrue(StringUtils.isBlank(" "));
    assertTrue(StringUtils.isBlank(""));
  }

  @Test
  public void testStartsWith() {
    assertFalse(StringUtils.startsWith("!", "something"));

    assertTrue(StringUtils.startsWith("!!!!!test", "!!!!"));
    assertTrue(StringUtils.startsWith("!something", "!"));
    assertTrue(StringUtils.startsWith(null, null));
  }

  @Test
  public void testPadRight() {
    assertEquals("sample", StringUtils.padRight("sample", 0, '0'));
    assertEquals("sample0000", StringUtils.padRight("sample", 10, '0'));
    assertEquals("0000000000", StringUtils.padRight("", 10, '0'));

    assertNull(StringUtils.padRight(null, 0, '0'));
  }

  @Test
  public void testPadLeft() {
    assertEquals("sample", StringUtils.padLeft("sample", 0, '0'));
    assertEquals("0000sample", StringUtils.padLeft("sample", 10, '0'));
    assertEquals("0000000000", StringUtils.padLeft("", 10, '0'));

    assertNull(StringUtils.padLeft(null, 0, '0'));
  }

  @Test
  public void testIsEmpty() {
    assertFalse(StringUtils.isEmpty("AAAAAAAA"));
    assertFalse(StringUtils.isEmpty(" "));

    assertTrue(StringUtils.isEmpty(""));
    assertTrue(StringUtils.isEmpty(null));
  }

  @Test
  public void testSubstringAfter() {
    assertEquals("", StringUtils.substringAfter("", ""));
    assertEquals("", StringUtils.substringAfter("", ">>"));
    assertEquals("after", StringUtils.substringAfter("substring>>after", ">>"));
    assertEquals("after>>another", StringUtils.substringAfter("substring>>after>>another", ">>"));
    assertEquals("", StringUtils.substringAfter("substring>>after", null));
    assertEquals("", StringUtils.substringAfter("substring.after", ">>"));
  }

  @Test
  public void testJoin() {
    final ArrayList<String> collection = new ArrayList<>();
    assertEquals("", StringUtils.join(collection, ","));

    collection.add("test1");
    assertEquals("test1", StringUtils.join(collection, ","));

    collection.add("test2");
    assertEquals("test1,test2", StringUtils.join(collection, ","));

    collection.add(null);
    assertEquals("test1,test2,null", StringUtils.join(collection, ","));
  }
}
