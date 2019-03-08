package org.apache.nifi.util;

public class FileExpansionUtil {

      public static String TILDE_CHAR_REGEX = "^~";

    /**
     * Pass in a path name consisting of ~/somedirectory and return the expanded version of this.
     * So if ~ expands to /Users/testuser then the expansion will provide /Users/testuser/somedirectory.
     *
     * @param pathname string
     * @return expanded pathname
     */
    public static String expandPath(String pathname) {
        String result = null;
        if (pathname != null) {
            result = pathname.replaceFirst(TILDE_CHAR_REGEX, System.getProperty("user.home"));
        }
        return result;
    }

}
