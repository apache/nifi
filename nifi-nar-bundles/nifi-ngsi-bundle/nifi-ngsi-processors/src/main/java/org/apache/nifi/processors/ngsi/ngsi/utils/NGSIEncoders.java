package org.apache.nifi.processors.ngsi.ngsi.utils;

import java.util.regex.Pattern;

public class NGSIEncoders {

    private static final Pattern ENCODEPOSTGRESQL = Pattern.compile("[^a-zA-Z0-9\\/]");

    /**
     * Encodes a string replacing all the non alphanumeric characters by '_' (except by '-' and '.').
     * This should be only called when building a persistence element name, such as table names, file paths, etc.
     *
     * @param in
     * @return The encoded version of the input string.
     */
    public static String encodePostgreSQL(String in) {
        return ENCODEPOSTGRESQL.matcher(in).replaceAll("_");
    } // encode

}
