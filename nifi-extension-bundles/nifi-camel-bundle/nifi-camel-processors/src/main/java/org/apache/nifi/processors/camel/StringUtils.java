package org.apache.nifi.processors.camel;

public class StringUtils {
    public static String truncateString(String s, int maxLength) {
        if (s.length() <= maxLength) {
            return s;
        } else {
            return s.substring(0, maxLength / 2) + "..." + s.substring(s.length() - 1 - maxLength / 2, s.length() - 1);
        }
    }
}
