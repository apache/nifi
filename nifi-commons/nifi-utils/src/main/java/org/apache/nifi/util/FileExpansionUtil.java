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
        String userHome = System.getProperty("user.home");

        if (userHome != null && userHome.trim().equals("")) {
            throw new RuntimeException("Nifi assumes user.home is set to your home directory.  Nifi detected user.home is " +
                    "either null or empty and this means your environment can't determine a value for this information.  " +
                "You can get around this by specifying a -Duser.home=<your home directory> when running nifi.");
        }

        if (pathname != null) {
            result = pathname.replaceFirst(TILDE_CHAR_REGEX, userHome);
        }
        return result;
    }

}
