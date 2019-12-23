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
package org.apache.nifi.util.text;

/**
 * <p>
 * A utility class that can be used to determine whether or not a String matches a given date/time format, as specified
 * by the Time Format used in {@link java.text.SimpleDateFormat}. It is not uncommon to see code written along the lines of:
 * </p>
 *
 * <code><pre>
 * final String format = "yyyy/MM/dd HH:mm:ss.SSS";
 * try {
 *     new SimpleDateFormat(format).parse(text);
 *     return true;
 * } catch (Exception e) {
 *     return false;
 * }
 * </pre></code>
 *
 * <p>
 *     This approach, however, is frowned upon for two important reasons. Firstly, the performance is poor. A micro-benchmark that involves executing
 *     the above code (even reusing the SimpleDateFormat object) to evaluate whether or not <code>text</code> is a timestamp took approximately 125-130 seconds
 *     to iterate 1,000,000 times (after discarding the first 1,000,000 iterations as a 'warmup'). As a comparison, this utility takes about 8-11 seconds against
 *     the same data and on the same machine.
 * </p>
 *
 * <p>
 *     Secondly, the above snippet has a very expensive side effect of throwing an Exception if the text does not match the format. This Exception is silently ignored,
 *     but can have devastating effects on the JVM as a whole, as creating the Exception object can result in requiring a Safepoint, which means that all threads in the JVM
 *     may be forced to pause.
 * </p>
 *
 * <p>
 *     Note, however, that this class is not intended to replace SimpleDateFormat, as it does not perform the actual parsing but instead only determines whether or not
 *     a given input text matches the pattern, so that if it does, a SimpleDateFormat can be used parse the input.
 * </p>
 */
public interface DateTimeMatcher {
    /**
     * Determines whether or not the text matches the pattern
     * @param text the text to evaluate
     * @return <code>true</code> if the text matches the pattern, <code>false</code> otherwise
     */
    boolean matches(String text);

    static DateTimeMatcher compile(String format) {
        if (format == null) {
            return t -> false;
        }

        return new DateTimeMatcherCompiler().compile(format);
    }
}
