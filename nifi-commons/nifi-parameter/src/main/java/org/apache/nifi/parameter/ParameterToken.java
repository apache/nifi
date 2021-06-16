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
package org.apache.nifi.parameter;

/**
 * A token encountered when parsing Strings for Parameter references. A Token may be a reference to a Parameter,
 * or it may be an "escaped reference" / non-reference.
 */
public interface ParameterToken {
    /**
     * @return the 0-based index in the String at which the token begins
     */
    int getStartOffset();

    /**
     * @return the 0-based index in the String at which the token ends
     */
    int getEndOffset();

    /**
     * @return the portion of the input text that corresponds to this token
     */
    String getText();

    /**
     * @return <code>true</code> if this token represents an escape sequence such as ##{param} or ## in the case of ###{param} or ####{param}, false if this
     * token does not represent an escape sequence.
     */
    boolean isEscapeSequence();

    /**
     * @return <code>true</code> if this token represents a reference to a Parameter. If this method returns <code>true</code>, then this token can be cast
     * as a {@link ParameterReference}.
     */
    boolean isParameterReference();

    /**
     * Returns the 'value' of the token. If this token is a parameter reference, it will return the value of the
     * Parameter, according to the given Parameter Context. If this token is an Escape Sequence, it will return the
     * un-escaped version of the escape sequence.
     * @param lookup the Parameter Lookup to use for looking up values
     * @return the value of the Token
     */
    String getValue(ParameterLookup lookup);
}
