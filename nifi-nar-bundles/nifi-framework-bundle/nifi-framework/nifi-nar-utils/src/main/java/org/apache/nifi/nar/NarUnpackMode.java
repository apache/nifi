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

package org.apache.nifi.nar;

/**
 * Specifies how the contents of a NAR file should be unpacked on disk.
 */
public enum NarUnpackMode {
    /**
     * Each JAR file in the NAR should be written out as a separate JAR file on disk.
     * This is generally faster, but in order to use a URLClassLoader to load all JARs,
     * many open file handles may be required.
     */
    UNPACK_INDIVIDUAL_JARS,

    /**
     * Each JAR file in the NAR should be combined into a single uber JAR file on disk.
     * Unpacking to an uber jar is generally a slower process. However, it has the upside of
     * being able to load all classes from the NAR using a single open file handle by the URLClassLoader.
     */
    UNPACK_TO_UBER_JAR;
}
