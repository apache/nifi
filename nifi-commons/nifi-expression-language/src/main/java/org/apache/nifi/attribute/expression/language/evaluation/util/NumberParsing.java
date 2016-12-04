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
    private static final String OptionalSign  = "[\\-\\+]?";

    private static final String Infinity = "(Infinity)";
    private static final String NotANumber = "(NaN)";

    // Base 10
    private static final String Base10Digits  = "\\d+";
    private static final String Base10Decimal  = "\\." + Base10Digits;
    private static final String OptionalBase10Decimal  = Base10Decimal + "?";

    private static final String Base10Exponent      = "[eE]" + OptionalSign + Base10Digits;
    private static final String OptionalBase10Exponent = "(" + Base10Exponent + ")?";

    // Hex
    private static final String HexIdentifier = "0[xX]";

    private static final String HexDigits     = "[0-9a-fA-F]+";
    private static final String HexDecimal = "\\." + HexDigits;
    private static final String OptionalHexDecimal = HexDecimal + "?";

    private static final String HexExponent      = "[pP]" + OptionalSign + Base10Digits;
    private static final String OptionalHexExponent = "(" + HexExponent + ")?";

    // Written according to the "Floating Point Literal" specification as outlined here: http://docs.oracle.com/javase/specs/jls/se8/html/jls-3.html#jls-3.10.2

    private static final String  doubleRegex =
            OptionalSign +
            "(" +
                Infinity + "|" +
                NotANumber + "|"+
                "(" + Base10Digits + Base10Decimal + ")" + "|" +
                "(" + Base10Digits + OptionalBase10Decimal + Base10Exponent + ")" + "|" +
                "(" + Base10Decimal + OptionalBase10Exponent + ")" + "|" +
                // The case of a hex number with a decimal portion but no exponent is not supported by "parseDouble" and throws a NumberFormatException
                "(" + HexIdentifier + HexDigits + "\\.?" + HexExponent + ")" + "|" + // The case of a hex numeral with a "." but no decimal values is valid.
                "(" + HexIdentifier + HexDigits + OptionalHexDecimal + HexExponent + ")" + "|" +
                "(" + HexIdentifier + HexDecimal + OptionalHexExponent + ")" +
            ")";

    private static final String numberRegex =
            OptionalSign +
            "(" +
                Base10Digits + "|" +
                HexIdentifier + HexDigits +
            ")";

    private static final Pattern DOUBLE_PATTERN = Pattern.compile(doubleRegex);
    private static final Pattern NUMBER_PATTERN = Pattern.compile(numberRegex);

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
