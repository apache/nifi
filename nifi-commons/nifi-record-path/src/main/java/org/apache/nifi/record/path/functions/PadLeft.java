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

package org.apache.nifi.record.path.functions;

import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.util.StringUtils;

public class PadLeft extends Padding {

    public PadLeft( final RecordPathSegment inputStringPath,
                    final RecordPathSegment desiredLengthPath,
                    final RecordPathSegment paddingStringPath,
                    final boolean absolute) {
        super("padLeft", null, inputStringPath, desiredLengthPath, paddingStringPath, absolute);
    }

    public PadLeft( final RecordPathSegment inputStringPath,
                    final RecordPathSegment desiredLengthPath,
                    final boolean absolute) {
        super("padLeft", null, inputStringPath, desiredLengthPath, null, absolute);
    }

    @Override
    protected String doPad(String inputString, int desiredLength, String pad) {
//
//        final int padLen = pad.length();
//        final int strLen = inputString.length();
//        final int pads = desiredLength - strLen;
//        if (pads <= 0 || desiredLength >= Integer.MAX_VALUE) {
//            return inputString; // returns original String when possible
//        }
//        if (padLen == 1 && pads <= PAD_LIMIT) {
//            return leftPad(str, size, padStr.charAt(0));
//        }
//
//        if (pads == padLen) {
//            return padStr.concat(str);
//        } else if (pads < padLen) {
//            return padStr.substring(0, pads).concat(str);
//        } else {
//            final char[] padding = new char[pads];
//            final char[] padChars = padStr.toCharArray();
//            for (int i = 0; i < pads; i++) {
//                padding[i] = padChars[i % padLen];
//            }
//            return new String(padding).concat(str);
//        }
//

        return StringUtils.padLeft(inputString, desiredLength, pad.charAt(0));
    }
}
