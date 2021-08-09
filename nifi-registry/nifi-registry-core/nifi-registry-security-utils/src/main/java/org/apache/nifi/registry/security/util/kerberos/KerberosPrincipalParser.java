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
package org.apache.nifi.registry.security.util.kerberos;

import org.apache.commons.lang3.StringUtils;

public class KerberosPrincipalParser {

    /**
     * <p>Determines the realm specified in the given kerberos principal.
     *
     * <p>The content of the given {@code principal} after the occurrence
     * of the last non-escaped realm delimiter ("@") will be considered
     * the realm of the principal.
     *
     * <p>The validity of the given {@code principal} and the determined realm
     * is not be verified by this method.
     *
     * @param principal the principal for which the realm will be determined
     * @return the realm of the given principal
     */
    public static String getRealm(String principal) {
        if (StringUtils.isBlank(principal)) {
            throw new IllegalArgumentException("principal can not be null or empty");
        }

        char previousChar = 0;
        int realmDelimiterIndex = -1;
        char currentChar;
        boolean realmDelimiterFound = false;
        int principalLength = principal.length();

        // find the last non-escaped occurrence of the realm delimiter
        for (int i = 0; i < principalLength; ++i) {
            currentChar = principal.charAt(i);
            if (currentChar == '@' && previousChar != '\\' ) {
                realmDelimiterIndex = i;
                realmDelimiterFound = true;
            }
            previousChar = currentChar;
        }

        String principalAfterLastRealmDelimiter = principal.substring(realmDelimiterIndex + 1);
        return realmDelimiterFound && realmDelimiterIndex + 1 < principalLength ? principalAfterLastRealmDelimiter : null;
    }
}
