package org.apache.nifi.processors.aws.wag.client;

import com.amazonaws.util.StringUtils;

public class Validate {
    public static void notEmpty(String in, String fieldName) {
        if (StringUtils.isNullOrEmpty(in)) {
            throw new IllegalArgumentException(String.format("%s cannot be empty", fieldName));
        }
    }

    public static void notNull(Object in, String fieldName) {
        if (in == null) {
            throw new IllegalArgumentException(String.format("%s cannot be null", fieldName));
        }
    }
}
