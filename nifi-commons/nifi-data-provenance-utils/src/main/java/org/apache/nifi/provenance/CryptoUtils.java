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
package org.apache.nifi.provenance;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.crypto.Cipher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoUtils {
    private static final Logger logger = LoggerFactory.getLogger(StaticKeyProvider.class);

    public static boolean isUnlimitedStrengthCryptoAvailable() {
        try {
            return Cipher.getMaxAllowedKeyLength("AES") > 128;
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Tried to determine if unlimited strength crypto is available but the AES algorithm is not available");
            return false;
        }
    }

    /**
     * Utility method which returns true if the string is null, empty, or entirely whitespace.
     *
     * @param src the string to evaluate
     * @return true if empty
     */
    public static boolean isEmpty(String src) {
        return src == null || src.trim().isEmpty();
    }

    /**
     * Returns a String that contains all of the elements of {@code list} delimited by {@code ","}. Any existing "," in one of the list elements will be escaped to {@code "\,"}.
     *
     * @param list the list of elements to serialize
     * @return the list as a single String
     */
    public static String serializeList(List<String> list) {
        return serializeList(list, ",");
    }

    /**
     * Returns a String that contains all of the elements of {@code list} delimited by
     * {@code delimiter}. Any existing {@code delimiter} in one of the list elements will be escaped to {@code Pattern.quote(delimiter)}.
     *
     * @param list      the list of elements to serialize
     * @param delimiter the delimiter
     * @return the list as a single String
     */
    public static String serializeList(List<String> list, String delimiter) {
        if (list == null || list.isEmpty()) {
            return "";
        } else {
            List<String> escapedList = list.stream().map(s -> escapeDelimiter(s, delimiter)).collect(Collectors.toList());
            return String.join(delimiter, escapedList);
        }
    }

    /**
     * Returns a List that contains all of the elements of {@code str} split by {@code ","}.
     * Any existing {@code "\,"} that was previously escaped during serialization in one of the list elements will be unescaped to {@code ","}.
     *
     * @param str the string to deserialize
     * @return the list of delimited elements
     */
    public static List<String> deserializeList(String str) {
        return deserializeList(str, ",");
    }

    /**
     * Returns a List that contains all of the elements of {@code str} split by {@code delimiter}.
     * Any existing {@code delimiter} that was previously escaped during serialization in one of the list elements will be unescaped via {@code Pattern.quote(delimiter)}.
     *
     * @param str       the string to deserialize
     * @param delimiter the delimiter
     * @return the list of delimited elements
     */
    public static List<String> deserializeList(String str, String delimiter) {
        if (isEmpty(str)) {
            return new ArrayList<>(0);
        } else {
            List<String> unescapedList = Arrays.asList(str.split(Pattern.quote(delimiter))).stream().map(s -> unescapeDelimiter(s, delimiter)).collect(Collectors.toList());
            return unescapedList;
        }
    }

    // TODO: Add de/serialize Map

    /**
     * Returns an escaped version of {@code str} with every instance of {@code delimiter} replaced with the standard {@code Pattern.quote(delimiter)} version.
     * <p>
     * Example:
     * <p>
     * escapeDelimiter("This,string,has,commas", ",") -> "This\,string\,has\,commas"
     *
     * @param str       the source string
     * @param delimiter the delimiter string
     * @return an escaped version of str
     */
    public static String escapeDelimiter(String str, String delimiter) {
        if (isEmpty(str)) {
            return str;
        } else if (str.contains(delimiter)) {
            String escapedDelimiter = Pattern.quote(delimiter);
            // Replace with CharSequence does literal replace ALL
            str = str.replace(delimiter, escapedDelimiter);
        }
        return str;
    }

    /**
     * Returns an unescaped version of {@code str} with every instance of {@code Pattern.quote(delimiter)} replaced with the original {@code delimiter}.
     * <p>
     * Example:
     * <p>
     * unescapeDelimiter("This\,string\,has\,commas", ",") -> "This,string,has,commas"
     *
     * @param str       the source string
     * @param delimiter the delimiter string
     * @return an unescaped version of str
     */
    public static String unescapeDelimiter(String str, String delimiter) {
        if (isEmpty(str)) {
            return str;
        } else {
            String escapedDelimiter = Pattern.quote(delimiter);
            if (str.contains(escapedDelimiter)) {
                // Replace with CharSequence does literal replace ALL
                str = str.replace(escapedDelimiter, delimiter);
            }
        }
        return str;
    }

    public static byte[] concatByteArrays(byte[]... arrays) throws IOException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        for (byte[] arr : arrays) {
            boas.write(arr);
        }
        return boas.toByteArray();
    }
}
