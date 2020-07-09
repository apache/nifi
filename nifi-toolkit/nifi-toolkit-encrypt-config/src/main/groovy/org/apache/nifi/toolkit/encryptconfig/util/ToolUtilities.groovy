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
package org.apache.nifi.toolkit.encryptconfig.util

import org.apache.commons.cli.CommandLine
import org.apache.commons.codec.binary.Hex
import org.apache.nifi.util.console.TextDevice
import org.apache.nifi.util.console.TextDevices
import org.bouncycastle.crypto.generators.SCrypt
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.nio.charset.StandardCharsets
import java.security.KeyException

class ToolUtilities {

    private static final Logger logger = LoggerFactory.getLogger(ToolUtilities.class)

    private static final int DEFAULT_MIN_PASSWORD_LENGTH = 12

    // Strong parameters as of 12 Aug 2016
    private static final int SCRYPT_N = 2**16
    private static final int SCRYPT_R = 8
    private static final int SCRYPT_P = 1

    static boolean isExactlyOneOptionSet(CommandLine commandLine, String... opt) {
        Collection<Boolean> setOptions = opt.findAll{commandLine.hasOption(it)}
        return setOptions.size() == 1
    }

    static boolean isExactlyOneTrue(Boolean... b) {
        Collection<Boolean> trues = b.findAll{it}
        return trues.size() == 1
    }

    /**
     * Helper method which returns true if the provided file exists and is readable
     *
     * @param fileToRead the proposed file to read
     * @return true if the caller should be able to successfully read from this file
     */
    static boolean canRead(File fileToRead) {
        fileToRead && (fileToRead.exists() && fileToRead.canRead())
    }

    /**
     * Helper method which returns true if it is "safe" to write to the provided file.
     *
     * Conditions:
     *  file does not exist and the parent directory is writable
     *  -OR-
     *  file exists and is writable
     *
     * @param fileToWrite the proposed file to be written to
     * @return true if the caller can "safely" write to this file location
     */
    static boolean isSafeToWrite(File fileToWrite) {
        fileToWrite && ((!fileToWrite.exists() && fileToWrite.absoluteFile.parentFile.canWrite()) || (fileToWrite.exists() && fileToWrite.canWrite()))
    }


    /**
     * The method returns the provided, derived, or securely-entered key in hex format.
     *
     * @param device
     * @param keyHex
     * @param password
     * @param usingPassword
     * @return
     */
    static String determineKey(TextDevice device = TextDevices.defaultTextDevice(), String keyHex, String password, boolean usingPassword) {
        if (usingPassword) {
            if (!password) {
                logger.debug("Reading password from secure console")
                password = readPasswordFromConsole(device)
            }
            keyHex = deriveKeyFromPassword(password)
            password = null
            return keyHex
        } else {
            if (!keyHex) {
                logger.debug("Reading hex key from secure console")
                keyHex = readKeyFromConsole(device)
            }
            return keyHex
        }
    }

    private static String readKeyFromConsole(TextDevice textDevice) {
        textDevice.printf("Enter the root key in hexadecimal format (spaces acceptable): ")
        new String(textDevice.readPassword())
    }

    private static String readPasswordFromConsole(TextDevice textDevice) {
        textDevice.printf("Enter the password: ")
        new String(textDevice.readPassword())
    }

//    /**
//     * Returns the key in uppercase hexadecimal format with delimiters (spaces, '-', etc.) removed. All non-hex chars are removed. If the result is not a valid length (32, 48, 64 chars depending on the JCE), an exception is thrown.
//     *
//     * @param rawKey the unprocessed key input
//     * @return the formatted hex string in uppercase
//     * @throws java.security.KeyException if the key is not a valid length after parsing
//     */
//    public static String parseKey(String rawKey) throws KeyException {
//        String hexKey = rawKey.replaceAll("[^0-9a-fA-F]", "")
//        def validKeyLengths = getValidKeyLengths()
//        if (!validKeyLengths.contains(hexKey.size() * 4)) {
//            throw new KeyException("The key (${hexKey.size()} hex chars) must be of length ${validKeyLengths} bits (${validKeyLengths.collect { it / 4 }} hex characters)")
//        }
//        hexKey.toUpperCase()
//    }

    /**
     * Returns the list of acceptable key lengths in bits based on the current JCE policies.
     *
     * @return 128 , [192, 256]
     */
    static List<Integer> getValidKeyLengths() {
        Cipher.getMaxAllowedKeyLength("AES") > 128 ? [128, 192, 256] : [128]
    }

    private static String deriveKeyFromPassword(String password, int minPasswordLength = DEFAULT_MIN_PASSWORD_LENGTH) {
        password = password?.trim()
        if (!password || password.length() < minPasswordLength) {
            throw new KeyException("Cannot derive key from empty/short password -- password must be at least ${minPasswordLength} characters")
        }

        // Generate a 128 bit salt
        byte[] salt = generateScryptSalt()
        int keyLengthInBytes = getValidKeyLengths().max() / 8
        byte[] derivedKeyBytes = SCrypt.generate(password.getBytes(StandardCharsets.UTF_8), salt, SCRYPT_N, SCRYPT_R, SCRYPT_P, keyLengthInBytes)
        Hex.encodeHexString(derivedKeyBytes).toUpperCase()
    }

    private static byte[] generateScryptSalt() {
//        byte[] salt = new byte[16]
//        new SecureRandom().nextBytes(salt)
//        salt
        /* It is not ideal to use a static salt, but the KDF operation must be deterministic
        for a given password, and storing and retrieving the salt in bootstrap.conf causes
        compatibility concerns
        */
        "NIFI_SCRYPT_SALT".getBytes(StandardCharsets.UTF_8)
    }

}
