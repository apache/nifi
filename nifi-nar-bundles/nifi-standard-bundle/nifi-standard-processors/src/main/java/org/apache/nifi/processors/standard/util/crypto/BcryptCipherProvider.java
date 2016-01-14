///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.nifi.processors.standard.util.crypto;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.crypto.Cipher;
//
//public class BcryptCipherProvider implements PBECipherProvider {
//    private static final Logger logger = LoggerFactory.getLogger(BcryptCipherProvider.class);
//
//    private final int workFactor;
//
//    public BcryptCipherProvider(int workFactor) {
//        this.workFactor = workFactor;
//    }
//
//    /**
//     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived by the KDF of the implementation.
//     *
//     * @param algorithm   the algorithm name
//     * @param provider    the provider name
//     * @param password    the secret input
//     * @param encryptMode true for encrypt, false for decrypt
//     * @return the initialized cipher
//     * @throws Exception if there is a problem initializing the cipher
//     */
//    @Override
//    public Cipher getCipher(String algorithm, String provider, String password, boolean encryptMode) throws Exception {
//        return null;
//    }
//
//    /**
//     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived by the KDF of the implementation.
//     *
//     * @param algorithm   the algorithm name
//     * @param provider    the provider name
//     * @param password    the secret input
//     * @param salt        the salt
//     * @param encryptMode true for encrypt, false for decrypt
//     * @return the initialized cipher
//     * @throws Exception if there is a problem initializing the cipher
//     */
//    @Override
//    public Cipher getCipher(String algorithm, String provider, String password, byte[] salt, boolean encryptMode) throws Exception {
//        return null;
//    }
//}
