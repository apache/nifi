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
package org.apache.nifi.security.util.crypto.scrypt;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.System.arraycopy;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.crypto.CipherUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Copyright (C) 2011 - Will Glozer.  All rights reserved.
 * <p/>
 * Taken from Will Glozer's port of Colin Percival's C implementation. Glozer's project located at <a href="https://github.com/wg/scrypt">https://github.com/wg/scrypt</a> was released under the ASF
 * 2.0 license and has not been updated since May 25, 2013 and there are outstanding issues which have been patched in this version.
 * <p/>
 * An implementation of the <a href="http://www.tarsnap.com/scrypt/scrypt.pdf">scrypt</a>
 * key derivation function.
 * <p/>
 * Allows for hashing passwords using the
 * <a href="http://www.tarsnap.com/scrypt.html">scrypt</a> key derivation function
 * and comparing a plain text password to a hashed one.
 */
public class Scrypt {
    private static final Logger logger = LoggerFactory.getLogger(Scrypt.class);

    private static final int DEFAULT_SALT_LENGTH = 16;

    /**
     * Hash the supplied plaintext password and generate output in the format described
     * below:
     * <p/>
     * The hashed output is an
     * extended implementation of the Modular Crypt Format that also includes the scrypt
     * algorithm parameters.
     * <p/>
     * Format: <code>$s0$PARAMS$SALT$KEY</code>.
     * <p/>
     * <dl>
     * <dd>PARAMS</dd><dt>32-bit hex integer containing log2(N) (16 bits), r (8 bits), and p (8 bits)</dt>
     * <dd>SALT</dd><dt>base64-encoded salt</dt>
     * <dd>KEY</dd><dt>base64-encoded derived key</dt>
     * </dl>
     * <p/>
     * <code>s0</code> identifies version 0 of the scrypt format, using a 128-bit salt and 256-bit derived key.
     * <p/>
     * This method generates a 16 byte random salt internally.
     *
     * @param password password
     * @param n        CPU cost parameter
     * @param r        memory cost parameter
     * @param p        parallelization parameter
     * @param dkLen    the desired key length in bits
     * @return the hashed password
     */
    public static String scrypt(String password, int n, int r, int p, int dkLen) {
        byte[] salt = new byte[DEFAULT_SALT_LENGTH];
        new SecureRandom().nextBytes(salt);

        return scrypt(password, salt, n, r, p, dkLen);
    }

    /**
     * Hash the supplied plaintext password and generate output in the format described
     * in {@link Scrypt#scrypt(String, int, int, int, int)}.
     *
     * @param password password
     * @param salt     the raw salt (16 bytes)
     * @param n        CPU cost parameter
     * @param r        memory cost parameter
     * @param p        parallelization parameter
     * @param dkLen    the desired key length in bits
     * @return the hashed password
     */
    public static String scrypt(String password, byte[] salt, int n, int r, int p, int dkLen) {
        try {
            byte[] derived = deriveScryptKey(password.getBytes(StandardCharsets.UTF_8), salt, n, r, p, dkLen);

            return formatHash(salt, n, r, p, derived);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("JVM doesn't support SHA1PRNG or HMAC_SHA256?");
        }
    }

    public static String formatSalt(byte[] salt, int n, int r, int p) {
        String params = encodeParams(n, r, p);

        StringBuilder sb = new StringBuilder((salt.length) * 2);
        sb.append("$s0$").append(params).append('$');
        sb.append(CipherUtility.encodeBase64NoPadding(salt));

        return sb.toString();
    }

    private static String encodeParams(int n, int r, int p) {
        return Long.toString(log2(n) << 16L | r << 8 | p, 16);
    }

    private static String formatHash(byte[] salt, int n, int r, int p, byte[] derived) {
        StringBuilder sb = new StringBuilder((salt.length + derived.length) * 2);
        sb.append(formatSalt(salt, n, r, p)).append('$');
        sb.append(CipherUtility.encodeBase64NoPadding(derived));

        return sb.toString();
    }

    /**
     * Returns the expected memory cost of the provided parameters in bytes.
     *
     * @param n the N value, iterations >= 2
     * @param r the r value, block size >= 1
     * @param p the p value, parallelization factor >= 1
     * @return the memory cost in bytes
     */
    public static int calculateExpectedMemory(int n, int r, int p) {
        return 128 * r * n + 128 * r * p;
    }

    /**
     * Compare the supplied plaintext password to a hashed password.
     *
     * @param password plaintext password
     * @param hashed   scrypt hashed password
     * @return true if password matches hashed value
     */
    public static boolean check(String password, String hashed) {
        try {
            if (StringUtils.isEmpty(password)) {
                throw new IllegalArgumentException("Password cannot be empty");
            }

            if (StringUtils.isEmpty(hashed)) {
                throw new IllegalArgumentException("Hash cannot be empty");
            }

            String[] parts = hashed.split("\\$");

            if (parts.length != 5 || !parts[1].equals("s0")) {
                throw new IllegalArgumentException("Hash is not properly formatted");
            }

            List<Integer> splitParams = parseParameters(parts[2]);
            int n = splitParams.get(0);
            int r = splitParams.get(1);
            int p = splitParams.get(2);

            byte[] salt = Base64.decodeBase64(parts[3]);
            byte[] derived0 = Base64.decodeBase64(parts[4]);

            // Previously this was hard-coded to 32 bits but the publicly-available scrypt methods accept arbitrary bit lengths
            int hashLength = derived0.length * 8;
            byte[] derived1 = deriveScryptKey(password.getBytes(StandardCharsets.UTF_8), salt, n, r, p, hashLength);

            if (derived0.length != derived1.length) return false;

            int result = 0;
            for (int i = 0; i < derived0.length; i++) {
                result |= derived0[i] ^ derived1[i];
            }
            return result == 0;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("JVM doesn't support SHA1PRNG or HMAC_SHA256?");
        }
    }

    /**
     * Parses the individual values from the encoded params value in the modified-mcrypt format for the salt & hash.
     * <p/>
     * Example:
     * <p/>
     * Hash: $s0$e0801$epIxT/h6HbbwHaehFnh/bw$7H0vsXlY8UxxyW/BWx/9GuY7jEvGjT71GFd6O4SZND0
     * Params:   e0801
     * <p/>
     * N = 16384
     * r = 8
     * p = 1
     *
     * @param encodedParams the String representation of the second section of the mcrypt format hash
     * @return a list containing N, r, p
     */
    public static List<Integer> parseParameters(String encodedParams) {
        long params = Long.parseLong(encodedParams, 16);

        List<Integer> paramsList = new ArrayList<>(3);

        // Parse N, r, p from encoded value and add to return list
        paramsList.add((int) Math.pow(2, params >> 16 & 0xffff));
        paramsList.add((int) params >> 8 & 0xff);
        paramsList.add((int) params & 0xff);

        return paramsList;
    }

    private static int log2(int n) {
        int log = 0;
        if ((n & 0xffff0000) != 0) {
            n >>>= 16;
            log = 16;
        }
        if (n >= 256) {
            n >>>= 8;
            log += 8;
        }
        if (n >= 16) {
            n >>>= 4;
            log += 4;
        }
        if (n >= 4) {
            n >>>= 2;
            log += 2;
        }
        return log + (n >>> 1);
    }

    /**
     * Implementation of the <a href="http://www.tarsnap.com/scrypt/scrypt.pdf">scrypt KDF</a>.
     *
     * @param password password
     * @param salt     salt
     * @param n        CPU cost parameter
     * @param r        memory cost parameter
     * @param p        parallelization parameter
     * @param dkLen    intended length of the derived key in bits
     * @return the derived key
     * @throws GeneralSecurityException when HMAC_SHA256 is not available
     */
    protected static byte[] deriveScryptKey(byte[] password, byte[] salt, int n, int r, int p, int dkLen) throws GeneralSecurityException {
        if (n < 2 || (n & (n - 1)) != 0) {
            throw new IllegalArgumentException("N must be a power of 2 greater than 1");
        }

        if (r < 1) {
            throw new IllegalArgumentException("Parameter r must be 1 or greater");
        }

        if (p < 1) {
            throw new IllegalArgumentException("Parameter p must be 1 or greater");
        }

        if (n > MAX_VALUE / 128 / r) {
            throw new IllegalArgumentException("Parameter N is too large");
        }

        // Must be enforced before r check
        if (p > MAX_VALUE / 128) {
            throw new IllegalArgumentException("Parameter p is too large");
        }

        if (r > MAX_VALUE / 128 / p) {
            throw new IllegalArgumentException("Parameter r is too large");
        }

        if (password == null || password.length == 0) {
            throw new IllegalArgumentException("Password cannot be empty");
        }

        int saltLength = salt == null ? 0 : salt.length;
        if (salt == null || saltLength == 0) {
            // Do not enforce this check here. According to the scrypt spec, the salt can be empty. However, in the user-facing ScryptCipherProvider, enforce an arbitrary check to avoid empty salts
            logger.warn("An empty salt was used for scrypt key derivation");
//            throw new IllegalArgumentException("Salt cannot be empty");
            // as the Exception is not being thrown, prevent NPE if salt is null by setting it to empty array
            if( salt == null ) salt = new byte[]{};
        }

        if (saltLength < 8 || saltLength > 32) {
            // Do not enforce this check here. According to the scrypt spec, the salt can be empty. However, in the user-facing ScryptCipherProvider, enforce an arbitrary check of [8..32] bytes
            logger.warn("A salt of length {} was used for scrypt key derivation", saltLength);
//            throw new IllegalArgumentException("Salt must be between 8 and 32 bytes");
        }

        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(password, "HmacSHA256"));

        byte[] b = new byte[128 * r * p];
        byte[] xy = new byte[256 * r];
        byte[] v = new byte[128 * r * n];
        int i;

        pbkdf2(mac, salt, 1, b, p * 128 * r);

        for (i = 0; i < p; i++) {
            smix(b, i * 128 * r, r, n, v, xy);
        }

        byte[] dk = new byte[dkLen / 8];
        pbkdf2(mac, b, 1, dk, dkLen / 8);
        return dk;
    }

    /**
     * Implementation of PBKDF2 (RFC2898).
     *
     * @param alg   the HMAC algorithm to use
     * @param p     the password
     * @param s     the salt
     * @param c     the iteration count
     * @param dkLen the intended length, in octets, of the derived key
     * @return The derived key
     */
    private static byte[] pbkdf2(String alg, byte[] p, byte[] s, int c, int dkLen) throws GeneralSecurityException {
        Mac mac = Mac.getInstance(alg);
        mac.init(new SecretKeySpec(p, alg));
        byte[] dk = new byte[dkLen];
        pbkdf2(mac, s, c, dk, dkLen);
        return dk;
    }

    /**
     * Implementation of PBKDF2 (RFC2898).
     *
     * @param mac   the pre-initialized {@link Mac} instance to use
     * @param s     the salt
     * @param c     the iteration count
     * @param dk    the byte array that derived key will be placed in
     * @param dkLen the intended length, in octets, of the derived key
     * @throws GeneralSecurityException if the key length is too long
     */
    private static void pbkdf2(Mac mac, byte[] s, int c, byte[] dk, int dkLen) throws GeneralSecurityException {
        int hLen = mac.getMacLength();

        if (dkLen > (Math.pow(2, 32) - 1) * hLen) {
            throw new GeneralSecurityException("Requested key length too long");
        }

        byte[] U = new byte[hLen];
        byte[] T = new byte[hLen];
        byte[] block1 = new byte[s.length + 4];

        int l = (int) Math.ceil((double) dkLen / hLen);
        int r = dkLen - (l - 1) * hLen;

        arraycopy(s, 0, block1, 0, s.length);

        for (int i = 1; i <= l; i++) {
            block1[s.length + 0] = (byte) (i >> 24 & 0xff);
            block1[s.length + 1] = (byte) (i >> 16 & 0xff);
            block1[s.length + 2] = (byte) (i >> 8 & 0xff);
            block1[s.length + 3] = (byte) (i >> 0 & 0xff);

            mac.update(block1);
            mac.doFinal(U, 0);
            arraycopy(U, 0, T, 0, hLen);

            for (int j = 1; j < c; j++) {
                mac.update(U);
                mac.doFinal(U, 0);

                for (int k = 0; k < hLen; k++) {
                    T[k] ^= U[k];
                }
            }

            arraycopy(T, 0, dk, (i - 1) * hLen, (i == l ? r : hLen));
        }
    }

    private static void smix(byte[] b, int bi, int r, int n, byte[] v, byte[] xy) {
        int xi = 0;
        int yi = 128 * r;
        int i;

        arraycopy(b, bi, xy, xi, 128 * r);

        for (i = 0; i < n; i++) {
            arraycopy(xy, xi, v, i * (128 * r), 128 * r);
            blockmix_salsa8(xy, xi, yi, r);
        }

        for (i = 0; i < n; i++) {
            int j = integerify(xy, xi, r) & (n - 1);
            blockxor(v, j * (128 * r), xy, xi, 128 * r);
            blockmix_salsa8(xy, xi, yi, r);
        }

        arraycopy(xy, xi, b, bi, 128 * r);
    }

    private static void blockmix_salsa8(byte[] by, int bi, int yi, int r) {
        byte[] X = new byte[64];
        int i;

        arraycopy(by, bi + (2 * r - 1) * 64, X, 0, 64);

        for (i = 0; i < 2 * r; i++) {
            blockxor(by, i * 64, X, 0, 64);
            salsa20_8(X);
            arraycopy(X, 0, by, yi + (i * 64), 64);
        }

        for (i = 0; i < r; i++) {
            arraycopy(by, yi + (i * 2) * 64, by, bi + (i * 64), 64);
        }

        for (i = 0; i < r; i++) {
            arraycopy(by, yi + (i * 2 + 1) * 64, by, bi + (i + r) * 64, 64);
        }
    }

    private static int r(int a, int b) {
        return (a << b) | (a >>> (32 - b));
    }

    private static void salsa20_8(byte[] b) {
        int[] b32 = new int[16];
        int[] x = new int[16];
        int i;

        for (i = 0; i < 16; i++) {
            b32[i] = (b[i * 4 + 0] & 0xff) << 0;
            b32[i] |= (b[i * 4 + 1] & 0xff) << 8;
            b32[i] |= (b[i * 4 + 2] & 0xff) << 16;
            b32[i] |= (b[i * 4 + 3] & 0xff) << 24;
        }

        arraycopy(b32, 0, x, 0, 16);

        for (i = 8; i > 0; i -= 2) {
            x[4] ^= r(x[0] + x[12], 7);
            x[8] ^= r(x[4] + x[0], 9);
            x[12] ^= r(x[8] + x[4], 13);
            x[0] ^= r(x[12] + x[8], 18);
            x[9] ^= r(x[5] + x[1], 7);
            x[13] ^= r(x[9] + x[5], 9);
            x[1] ^= r(x[13] + x[9], 13);
            x[5] ^= r(x[1] + x[13], 18);
            x[14] ^= r(x[10] + x[6], 7);
            x[2] ^= r(x[14] + x[10], 9);
            x[6] ^= r(x[2] + x[14], 13);
            x[10] ^= r(x[6] + x[2], 18);
            x[3] ^= r(x[15] + x[11], 7);
            x[7] ^= r(x[3] + x[15], 9);
            x[11] ^= r(x[7] + x[3], 13);
            x[15] ^= r(x[11] + x[7], 18);
            x[1] ^= r(x[0] + x[3], 7);
            x[2] ^= r(x[1] + x[0], 9);
            x[3] ^= r(x[2] + x[1], 13);
            x[0] ^= r(x[3] + x[2], 18);
            x[6] ^= r(x[5] + x[4], 7);
            x[7] ^= r(x[6] + x[5], 9);
            x[4] ^= r(x[7] + x[6], 13);
            x[5] ^= r(x[4] + x[7], 18);
            x[11] ^= r(x[10] + x[9], 7);
            x[8] ^= r(x[11] + x[10], 9);
            x[9] ^= r(x[8] + x[11], 13);
            x[10] ^= r(x[9] + x[8], 18);
            x[12] ^= r(x[15] + x[14], 7);
            x[13] ^= r(x[12] + x[15], 9);
            x[14] ^= r(x[13] + x[12], 13);
            x[15] ^= r(x[14] + x[13], 18);
        }

        for (i = 0; i < 16; ++i) b32[i] = x[i] + b32[i];

        for (i = 0; i < 16; i++) {
            b[i * 4 + 0] = (byte) (b32[i] >> 0 & 0xff);
            b[i * 4 + 1] = (byte) (b32[i] >> 8 & 0xff);
            b[i * 4 + 2] = (byte) (b32[i] >> 16 & 0xff);
            b[i * 4 + 3] = (byte) (b32[i] >> 24 & 0xff);
        }
    }

    private static void blockxor(byte[] s, int si, byte[] d, int di, int len) {
        for (int i = 0; i < len; i++) {
            d[di + i] ^= s[si + i];
        }
    }

    private static int integerify(byte[] b, int bi, int r) {
        int n;

        bi += (2 * r - 1) * 64;

        n = (b[bi + 0] & 0xff) << 0;
        n |= (b[bi + 1] & 0xff) << 8;
        n |= (b[bi + 2] & 0xff) << 16;
        n |= (b[bi + 3] & 0xff) << 24;

        return n;
    }

    public static int getDefaultSaltLength() {
        return DEFAULT_SALT_LENGTH;
    }
}