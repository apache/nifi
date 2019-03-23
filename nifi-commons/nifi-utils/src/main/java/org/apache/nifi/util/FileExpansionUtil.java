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

        if (userHome == null || userHome.trim().equals("")) {
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
