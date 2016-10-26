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

package org.apache.nifi.attribute.expression.language.evaluation.util;

import java.util.regex.Pattern;

public class NumberParsing {


    public static enum ParseResultType {
        NOT_NUMBER, WHOLE_NUMBER, DECIMAL;
    }
    private static final String Digits     = "(\\p{Digit}+)";

    // Double regex according to Oracle documentation: http://docs.oracle.com/javase/6/docs/api/java/lang/Double.html#valueOf%28java.lang.String%29
    private static final String HexDigits  = "(\\p{XDigit}+)";
    // an exponent is 'e' or 'E' followed by an optionally
    // signed decimal integer.
    private static final String Exp        = "[eE][+-]?"+Digits;
    private static final String fpRegex    =
            ("[\\x00-\\x20]*"+  // Optional leading "whitespace"
                    "[+-]?(" + // Optional sign character
                    "NaN|" +           // "NaN" string
                    "Infinity|" +      // "Infinity" string

                    // A decimal floating-point string representing a finite positive
                    // number without a leading sign has at most five basic pieces:
                    // Digits . Digits ExponentPart FloatTypeSuffix
                    //
                    // Since this method allows integer-only strings as input
                    // in addition to strings of floating-point literals, the
                    // two sub-patterns below are simplifications of the grammar
                    // productions from the Java Language Specification, 2nd
                    // edition, section 3.10.2.

                    // Digits ._opt Digits_opt ExponentPart_opt FloatTypeSuffix_opt
                    "((("+Digits+"(\\.)?("+Digits+"?)("+Exp+")?)|"+

                    // . Digits ExponentPart_opt FloatTypeSuffix_opt
                    "(\\.("+Digits+")("+Exp+")?)|"+

                    // Hexadecimal strings
                    "((" +
                    // 0[xX] HexDigits ._opt BinaryExponent FloatTypeSuffix_opt
                    "(0[xX]" + HexDigits + "(\\.)?)|" +

                    // 0[xX] HexDigits_opt . HexDigits BinaryExponent FloatTypeSuffix_opt
                    "(0[xX]" + HexDigits + "?(\\.)" + HexDigits + ")" +

                    ")[pP][+-]?" + Digits + "))" +
                    "[fFdD]?))" +
                    "[\\x00-\\x20]*");// Optional trailing "whitespace"

    private static final Pattern DOUBLE_PATTERN = Pattern.compile(fpRegex);

    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?((\\d+)|(0[xX]" + HexDigits + "))");

    private NumberParsing(){
    }

    public static ParseResultType parse(String input){
        if (NUMBER_PATTERN.matcher(input).matches()) {
            return ParseResultType.WHOLE_NUMBER;
        } else if (DOUBLE_PATTERN.matcher(input).matches()) {
            return ParseResultType.DECIMAL;
        } else {
            return ParseResultType.NOT_NUMBER;
        }
    }
}
