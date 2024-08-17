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
package org.apache.nifi.web.api.streaming;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard implementation of Byte Range Header Parser supporting one range specifier of bytes with int-range or suffix-range values
 */
public class StandardByteRangeParser implements ByteRangeParser {
    private static final Pattern BYTE_RANGE_PATTERN = Pattern.compile("^bytes=(0|[1-9][0-9]{0,18})?-(0|[1-9][0-9]{0,18})?$");

    private static final int FIRST_POSITION_GROUP = 1;

    private static final int LAST_POSITION_GROUP = 2;

    @Override
    public Optional<ByteRange> readByteRange(final String rangeHeader) {
        final ByteRange byteRange;

        if (rangeHeader == null || rangeHeader.isBlank()) {
            byteRange = null;
        } else {
            final Matcher matcher = BYTE_RANGE_PATTERN.matcher(rangeHeader);
            if (matcher.matches()) {
                final Long firstPosition;
                final Long lastPosition;

                final String firstPositionGroup = matcher.group(FIRST_POSITION_GROUP);
                final String lastPositionGroup = matcher.group(LAST_POSITION_GROUP);

                if (firstPositionGroup == null) {
                    if (lastPositionGroup == null) {
                        throw new ByteRangeFormatException("Range header missing first and last positions");
                    }
                    firstPosition = null;
                    lastPosition = parsePosition(lastPositionGroup);
                } else {
                    firstPosition = parsePosition(firstPositionGroup);
                    if (lastPositionGroup == null) {
                        lastPosition = null;
                    } else {
                        lastPosition = parsePosition(lastPositionGroup);

                        if (lastPosition < firstPosition) {
                            throw new ByteRangeFormatException("Range header not valid: last position less than first position");
                        }
                    }
                }

                byteRange = new ByteRange(firstPosition, lastPosition);
            } else {
                throw new ByteRangeFormatException("Range header not valid");
            }
        }

        return Optional.ofNullable(byteRange);
    }

    private long parsePosition(final String positionGroup) {
        try {
            return Long.parseLong(positionGroup);
        } catch (final NumberFormatException e) {
            throw new ByteRangeFormatException("Range header position not valid");
        }
    }
}
