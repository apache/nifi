package org.apache.nifi.serialization.record.field;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.regex.Matcher;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectZonedDateTimeConverterTest {
    @ParameterizedTest
    @MethodSource("getPatterns")
    void testTimeZonePattern(String pattern, boolean expected) {
        final Matcher matcher = ObjectZonedDateTimeConverter.TIMEZONE_PATTERN.matcher(pattern);
        if(expected) {
            assertTrue(matcher.find());
        } else {
            assertFalse(matcher.find());
        }
    }

    private static Stream<Arguments> getPatterns() {
        return Stream.of(
                Arguments.of("yyyy-MM-dd'T'HH:mm:ssZ", true),
                Arguments.of("Zyyyy-MM-dd'T'HH:mm:ss", true),
                Arguments.of("yyyy-MM-dd'T'ZHH:mm:ss", true),
                Arguments.of("yyyy-MM-ddZ'T'HH:mm:ss", true),
                Arguments.of("yyyy-MM-dd'T'HH:mm:ss'Z'", false),
                Arguments.of("EEEE, MMM dd, yyyy HH:mm:ss a", false),
                Arguments.of("dd-MMM-yyyy", false),
                Arguments.of("MMMM dd, yyyy: EEEE", false)
        );
    }
}
