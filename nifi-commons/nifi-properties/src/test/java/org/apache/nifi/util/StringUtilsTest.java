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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
    assertEquals("sample", StringUtils.rightPad("sample", 0, '0'));
    assertEquals("sample", StringUtils.rightPad("sample", -5, '0'));
    assertEquals("sample0000", StringUtils.rightPad("sample", 10, '0'));
    assertEquals("0000000000", StringUtils.rightPad("", 10, '0'));
    assertEquals("samplexyxy", StringUtils.rightPad("sample", 10, "xy")); // multiple pads
    assertEquals("samplexy", StringUtils.rightPad("sample", 8, "xyz"));   // less than 1 pad
    assertEquals("samplexy", StringUtils.rightPad("sample", 8, "xy"));    // exactly 1 pad
    assertEquals("sample    ", StringUtils.rightPad("sample", 10, null));     // null pad

    assertNull(StringUtils.rightPad(null, 0, '0'));
  }

  @Test
  public void testPadLeft() {
    assertEquals("sample", StringUtils.leftPad("sample", 0, '0'));
    assertEquals("0000sample", StringUtils.leftPad("sample", 10, '0'));
    assertEquals("0000000000", StringUtils.leftPad("", 10, '0'));
    assertEquals("xyxysample", StringUtils.leftPad("sample", 10, "xy"));  // multiple pads
    assertEquals("xysample", StringUtils.leftPad("sample", 8, "xyz"));    // less than 1 pad
    assertEquals("xysample", StringUtils.leftPad("sample", 8, "xy"));     // exactly 1 pad
    assertEquals("    sample", StringUtils.leftPad("sample", 10, null));      // null pad

    assertNull(StringUtils.leftPad(null, 0, '0'));
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

  @Test
  public void testShouldTitleCaseStrings() {
    // Arrange
    List<String> inputs = Arrays.asList(null, "", "  leading space", "trailing space  ", "multiple   spaces", "this is a sentence", "allOneWord", "PREVIOUSLY UPPERCASE");
    List<String> expected = Arrays.asList("", "", "Leading Space", "Trailing Space", "Multiple Spaces", "This Is A Sentence", "Alloneword", "Previously Uppercase");

    // Act
    List<String> titleCased = inputs.stream().map(StringUtils::toTitleCase).collect(Collectors.toList());

    // Assert
    assertEquals(titleCased, expected);
  }
}
