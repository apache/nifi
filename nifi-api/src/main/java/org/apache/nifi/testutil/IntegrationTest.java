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
package org.apache.nifi.testutil;

import org.junit.experimental.categories.Category;

/**
 * Marker interface that is used in parallel with JUnit's {@link Category}
 * annotation allowing you to annotate the entire tests or individual test
 * methods as Integration Tests thus excluding them from the main Maven build.
 *
 * <pre>
 * &#64;Test
 * &#64;Category(IntegrationTest.class)
 * public void myTest() throws Exception {
 *    . . .
 * }
 * </pre>
 *
 * Annotated tests will still run under JUnit without any modifications.
 *
 */
public interface IntegrationTest {

}
