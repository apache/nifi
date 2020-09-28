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
package org.apache.nifi.processors.jwt;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.junit.Assert;

public class VerifyJWTTest {

    final static String DEFAULT_PUB_KEY_EXTENSION = "";
    final static String DEFAULT_JWT_ATTRIBUTE_NAME = "";
    final static String PUBLIC_KEY_PATH = "./src/test/resources/";
    final static InputStream DUMMY_CONTENT = new ByteArrayInputStream("Dummy content".getBytes());
    final static String expectedJson = "{\"sub\":\"1234567890\",\"name\":\"John Doe\",\"admin\":true,\"iat\":1516239022,\"testComplexStruct\":{\"even more complex\":[\"and a\",\"bit more\"]}}";

    // All test JWTs were created using https://jwt.io using a key from ./resources
    @Test
    public void key1Signed_withPublic() throws IOException {
        final String jwt = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwidGVzdENvbXBsZXhTdHJ1Y3QiOnsiZXZlbiBtb3JlIG"
                + "NvbXBsZXgiOlsiYW5kIGEiLCJiaXQgbW9yZSJdfX0.hrMHJBMqZP8bQ67cSmgrc3729651JVOm3XncaKOt4EZ6KYsWA3VaXV4B7glEibBtNQRBnnw6IkbJ1Qyb5nABYBBPcUg0nB4WHLJM3eTp_rem15DQpY-sCVst3OOtSxhKa9ds9M8Ivs"
                + "VW1tZ2p9FKls5TXPFOD34lGW_4n0-Zrs6m8VpiFinPwrwN6WW1CaHQhXNrgLczYRtcvICZiydxNOvvY6Mh9PYymGMYiDMF9LUh9ilADXGk-CbZGckfRegFK0Gb2d1MVE8Py2fFcVrtYCOecL_9eeHrZ1YBxGIXa7fF-eUohvj5kuj_jTxne1"
                + "MIIYUlATSHIOMVLuwOGfS6kw";
        final InputStream content = new ByteArrayInputStream(jwt.getBytes());
        final String expectedHeader = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9";
        final String expectedFooter = "hrMHJBMqZP8bQ67cSmgrc3729651JVOm3XncaKOt4EZ6KYsWA3VaXV4B7glEibBtNQRBnnw6IkbJ1Qyb5nABYBBPcUg0nB4WHLJM3eTp_rem15DQpY-sCVst3OOtSxhKa9ds9M8IvsVW1tZ2p9FKls5TXPFOD34l"
                + "GW_4n0-Zrs6m8VpiFinPwrwN6WW1CaHQhXNrgLczYRtcvICZiydxNOvvY6Mh9PYymGMYiDMF9LUh9ilADXGk-CbZGckfRegFK0Gb2d1MVE8Py2fFcVrtYCOecL_9eeHrZ1YBxGIXa7fF-eUohvj5kuj_jTxne1MIIYUlATSHIOMVLuwOGfS6"
                + "kw";

        testSuccessfulBody(content, DEFAULT_PUB_KEY_EXTENSION, expectedHeader, expectedFooter);
    }

    // public key has different extension so cannot be validated
    @Test
    public void key1Signed_noPublicExtension() throws IOException {
        final String jwt = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwidGVzdENvbXBsZXhTdHJ1Y3QiOnsiZXZlbiBtb3JlIG"
                + "NvbXBsZXgiOlsiYW5kIGEiLCJiaXQgbW9yZSJdfX0.CHflrv9KZ7wOU4Fp1_PKuqE27pgND7SPs2GWuBnE44C2fMQKZwf7wqGCTYJ9AEGc6LH6ZFVWYMf2R8btgC1nbDHO8oAKWFfL2q70-ik_-I2JQuAAqOlTpfFMdC5WGrzKfyl_8l9E8F"
                + "EeN_-BHN37-XhH3tZvEMABBpuI0WrQcPT2UDi_ptVxMv0ZF9WFjIMxi43YUbhDHaMEzoMPQkTKgM2GrX7JU86wI-WGiPz9K4RiYlQa8l35CgOccSk45CGlyPnUndas8cbsS9DBDYwXAufxmtcIGC8JQzURVgmsvGoXQ5SWUnyuf98Y6NJ67C"
                + "poevoG5nlX74a96DQmWzZrqQ";
        final InputStream content = new ByteArrayInputStream(jwt.getBytes());

        testUnsuccessfulBody(content, ".*\\.pem");
    }

    // public key has different extension so cannot be validated
    @Test
    public void key3Signed_noPubExtension() throws IOException {
        final String jwt = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwidGVzdENvbXBsZXhTdHJ1Y3QiOnsiZXZlbiBtb3JlIG"
                + "NvbXBsZXgiOlsiYW5kIGEiLCJiaXQgbW9yZSJdfX0.iyMgMV_8w-QlF1cYBm9B1fgVfd84D4Z5kVxIUT6JZx77J5hrLOWhE12_W3c7FfwHc2u00IqDN2DwZ5blwQERFzBZwfPEfBMo6b68HPuDpCf_76tyivpAfHTLbFx1432fOKObBh87m_"
                + "SibAPV_b0hHEp71Usc-76Lmj3n_33QTYjhsiEHVex-FEtF3sHe3vJGJWK_VVkIL2qK-LEJ8Pk5iu4KHPEyRPFp2vUIhKdch3M7qnJ3AncuBIkcvgiHCfG1rHK8Sfz7FJiOpQm5tpUEFHilrmZGGUVT3rnLlK1z2EAOCiiNXBodC7UKvSrFva"
                + "xMCByNFThDjBZdpfqEnb0c8l5dRGe-gK7mqs1W1GvAjDYHLL-kowJoQOqFP1RQ2Lmy5oEwER0EQ6z3GOPpb5VLZxa_1DDnUKAgzroR16pVsgq-RLIOkMdNvjzeljeLFOURGy_5xoo1BonYVC853JOjtWbWia1DTw3m9YFYIpZr3cI8uugCV-"
                + "cfn1twkKT4RCVoZR6pvfSDv69HnQxbDBGQms13Es9CRZRHsykSAX75GDKus0s1ULmJzCrhcS2gplJ3ZxWtdmC30niT9BKeDHn2zexEp5UdHbSIiYQ0JG5lC6uS7T5RUcCz_URxEcyIu31XT5xIdV4oMuvfB3MhtNbroBFV8cS8w3Xuk1vR-U"
                + "N32q8";
        final InputStream content = new ByteArrayInputStream(jwt.getBytes());
        testUnsuccessfulBody(content, DEFAULT_PUB_KEY_EXTENSION);
    }

    // key in directory with proper extension given
    @Test
    public void key3Signed_correctExtension() throws IOException {
        final String jwt = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwidGVzdENvbXBsZXhTdHJ1Y3QiOnsiZXZlbiBtb3JlIG"
                + "NvbXBsZXgiOlsiYW5kIGEiLCJiaXQgbW9yZSJdfX0.clnMschjA9Ib2_9BI9nkpcIzwtF6cL_RbI7Xu6fDY3k5-6MfNX9wSYzYNXIiFxJjybltfW1YGBtrBcz4rMVx3NstACQLjS-wEglVzlTE9jID7GicSSFHsRUDVKFnFNFO8S-x5lxjB3"
                + "UZpeEQbngE-diI4gQPnR8otv8rFr2mtd6P_TUIoFSwVg8raSg8CXi5WJlCdm2Bd90JCSmsjoxLV4WHVB4WnOnXZloYPMBcy-F1xX9I0Za5JJwQXHEeVoXBhFsmEUk3tsli9asD5DXadIODSGChNyFh66l0Y1UYaQqf7r_eRVFFUqvz887uv-"
                + "OKjRYPO9QtNyooHW-8dm0ReLVVpVrOKixeaIVJqz2hGTdCR9Ws_MZ6nR1uo5KYFOixI7cII842je19DCf5xjggckAi8C2CM8NG-NldRbgsAQdkvBbSajqA34sfCzG-HJwexzVelXyUDCXw09Iy7Ra98oOImfeTRtMbNmPyNJk_W-raYeRPCH"
                + "uWX9hZgNpgSQ0tN0xAC1bTHVqqq2tGcO8FHfQxDkwaqUq56KccCRmrEG7RLqMb2dFnUWYD7ZwjGRgDbgtYfUvy-hGMweVIziFRY7fpP6labGOqQRRrSU3DO9at1vf69mM0a-nWMb3Ry8o_vjeggFYEIjdZBhWnQpefrZvxJpL0sNt4m3kUl8"
                + "d44iA";
        final InputStream content = new ByteArrayInputStream(jwt.getBytes());
        final String expectedHeader = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9";
        final String expectedFooter = "clnMschjA9Ib2_9BI9nkpcIzwtF6cL_RbI7Xu6fDY3k5-6MfNX9wSYzYNXIiFxJjybltfW1YGBtrBcz4rMVx3NstACQLjS-wEglVzlTE9jID7GicSSFHsRUDVKFnFNFO8S-x5lxjB3UZpeEQbngE-diI4gQPnR8o"
                + "tv8rFr2mtd6P_TUIoFSwVg8raSg8CXi5WJlCdm2Bd90JCSmsjoxLV4WHVB4WnOnXZloYPMBcy-F1xX9I0Za5JJwQXHEeVoXBhFsmEUk3tsli9asD5DXadIODSGChNyFh66l0Y1UYaQqf7r_eRVFFUqvz887uv-OKjRYPO9QtNyooHW-8dm0R"
                + "eLVVpVrOKixeaIVJqz2hGTdCR9Ws_MZ6nR1uo5KYFOixI7cII842je19DCf5xjggckAi8C2CM8NG-NldRbgsAQdkvBbSajqA34sfCzG-HJwexzVelXyUDCXw09Iy7Ra98oOImfeTRtMbNmPyNJk_W-raYeRPCHuWX9hZgNpgSQ0tN0xAC1bT"
                + "HVqqq2tGcO8FHfQxDkwaqUq56KccCRmrEG7RLqMb2dFnUWYD7ZwjGRgDbgtYfUvy-hGMweVIziFRY7fpP6labGOqQRRrSU3DO9at1vf69mM0a-nWMb3Ry8o_vjeggFYEIjdZBhWnQpefrZvxJpL0sNt4m3kUl8d44iA";
        testSuccessfulBody(content, ".*\\.pem", expectedHeader, expectedFooter);
    }

    // matching key in property
    @Test
    public void key3Signed_publicKeyInProperty() throws IOException {
        final String jwt = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwidGVzdENvbXBsZXhTdHJ1Y3QiOnsiZXZlbiBtb3JlIG"
                + "NvbXBsZXgiOlsiYW5kIGEiLCJiaXQgbW9yZSJdfX0.clnMschjA9Ib2_9BI9nkpcIzwtF6cL_RbI7Xu6fDY3k5-6MfNX9wSYzYNXIiFxJjybltfW1YGBtrBcz4rMVx3NstACQLjS-wEglVzlTE9jID7GicSSFHsRUDVKFnFNFO8S-x5lxjB3"
                + "UZpeEQbngE-diI4gQPnR8otv8rFr2mtd6P_TUIoFSwVg8raSg8CXi5WJlCdm2Bd90JCSmsjoxLV4WHVB4WnOnXZloYPMBcy-F1xX9I0Za5JJwQXHEeVoXBhFsmEUk3tsli9asD5DXadIODSGChNyFh66l0Y1UYaQqf7r_eRVFFUqvz887uv-"
                + "OKjRYPO9QtNyooHW-8dm0ReLVVpVrOKixeaIVJqz2hGTdCR9Ws_MZ6nR1uo5KYFOixI7cII842je19DCf5xjggckAi8C2CM8NG-NldRbgsAQdkvBbSajqA34sfCzG-HJwexzVelXyUDCXw09Iy7Ra98oOImfeTRtMbNmPyNJk_W-raYeRPCH"
                + "uWX9hZgNpgSQ0tN0xAC1bTHVqqq2tGcO8FHfQxDkwaqUq56KccCRmrEG7RLqMb2dFnUWYD7ZwjGRgDbgtYfUvy-hGMweVIziFRY7fpP6labGOqQRRrSU3DO9at1vf69mM0a-nWMb3Ry8o_vjeggFYEIjdZBhWnQpefrZvxJpL0sNt4m3kUl8"
                + "d44iA";
        final InputStream content = new ByteArrayInputStream(jwt.getBytes());
        final String expectedHeader = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9";
        final String expectedFooter = "clnMschjA9Ib2_9BI9nkpcIzwtF6cL_RbI7Xu6fDY3k5-6MfNX9wSYzYNXIiFxJjybltfW1YGBtrBcz4rMVx3NstACQLjS-wEglVzlTE9jID7GicSSFHsRUDVKFnFNFO8S-x5lxjB3UZpeEQbngE-diI4gQPnR8o"
                + "tv8rFr2mtd6P_TUIoFSwVg8raSg8CXi5WJlCdm2Bd90JCSmsjoxLV4WHVB4WnOnXZloYPMBcy-F1xX9I0Za5JJwQXHEeVoXBhFsmEUk3tsli9asD5DXadIODSGChNyFh66l0Y1UYaQqf7r_eRVFFUqvz887uv-OKjRYPO9QtNyooHW-8dm0R"
                + "eLVVpVrOKixeaIVJqz2hGTdCR9Ws_MZ6nR1uo5KYFOixI7cII842je19DCf5xjggckAi8C2CM8NG-NldRbgsAQdkvBbSajqA34sfCzG-HJwexzVelXyUDCXw09Iy7Ra98oOImfeTRtMbNmPyNJk_W-raYeRPCHuWX9hZgNpgSQ0tN0xAC1bT"
                + "HVqqq2tGcO8FHfQxDkwaqUq56KccCRmrEG7RLqMb2dFnUWYD7ZwjGRgDbgtYfUvy-hGMweVIziFRY7fpP6labGOqQRRrSU3DO9at1vf69mM0a-nWMb3Ry8o_vjeggFYEIjdZBhWnQpefrZvxJpL0sNt4m3kUl8d44iA";

        try {
            final File file = new File("./src/test/resources/key3.pem");
            final String asciiKey = new String(Files.readAllBytes(file.toPath()));

            TestRunner runner = setUp(DEFAULT_JWT_ATTRIBUTE_NAME, DEFAULT_PUB_KEY_EXTENSION, PUBLIC_KEY_PATH);
            runner.setProperty("Public Key", asciiKey);
            runner = run(runner, content);

            final MockFlowFile result = testSuccessful(runner);
            final String resultValue = new String(runner.getContentAsByteArray(result));

            // Test attributes and content
            Assert.assertEquals(expectedJson, resultValue);

            final Map<String, String> attributes = result.getAttributes();
            Assert.assertEquals(expectedHeader, attributes.get(VerifyJWT.HEADER_ATTRIBUTE));
            Assert.assertEquals(expectedFooter, attributes.get(VerifyJWT.FOOTER_ATTRIBUTE));
        } catch (final Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    // no matching key in directory or property
    @Test
    public void signedWithNoMatchingPublicKey() throws IOException {
        final String expectedString = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.POstGetfAytaZS82wHcjoTyoqhMyxXi"
                + "WdR7Nn7A29DNSl0EiXLdwJ6xC6AfgZWF1bOsS_TuYI3OG85AmiExREkrS6tDfTQ2B3WXlrr-wp5AokiRbz3_oB4OxG-W9KcEEbDRcZc0nH3L7LzYptiy1PtAylQGxHTWZXtGz4ht0bAecBgmpdgXMguEIcoqPJ1n3pIWk_dUZegpqx0Lka21"
                + "H6XxUTxiy8OcaarA8zdnPUnV6AmNP3ecFawIFYdvJB_cm-GvpCSbr8G8y_Mllj8f4x9nBH8pQux89_6gUY618iYv7tuPWBFfEbLxtF2pZS6YC1aSfLQxeNe8djT9YjpvRZA";
        final InputStream content = new ByteArrayInputStream(expectedString.getBytes());

        testUnsuccessfulBody(content, ".*");
    }

    // no keys given at all
    @Test
    public void invalidNoKeys() {
        final TestRunner runner  = setUp(DEFAULT_JWT_ATTRIBUTE_NAME, DEFAULT_JWT_ATTRIBUTE_NAME, PUBLIC_KEY_PATH);
        runner.setProperty(VerifyJWT.PUBLIC_KEYS_PATH, "");
        runner.assertNotValid();
    }

    // no directory given but correct key given in property
    @Test
    public void key1PubProperty_noDir() throws IOException {
        final String jwt = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwidGVzdENvbXBsZXhTdHJ1Y3QiOnsiZXZlbiBtb3JlIG"
                + "NvbXBsZXgiOlsiYW5kIGEiLCJiaXQgbW9yZSJdfX0.hrMHJBMqZP8bQ67cSmgrc3729651JVOm3XncaKOt4EZ6KYsWA3VaXV4B7glEibBtNQRBnnw6IkbJ1Qyb5nABYBBPcUg0nB4WHLJM3eTp_rem15DQpY-sCVst3OOtSxhKa9ds9M8Ivs"
                + "VW1tZ2p9FKls5TXPFOD34lGW_4n0-Zrs6m8VpiFinPwrwN6WW1CaHQhXNrgLczYRtcvICZiydxNOvvY6Mh9PYymGMYiDMF9LUh9ilADXGk-CbZGckfRegFK0Gb2d1MVE8Py2fFcVrtYCOecL_9eeHrZ1YBxGIXa7fF-eUohvj5kuj_jTxne1"
                + "MIIYUlATSHIOMVLuwOGfS6kw";
        final InputStream content = new ByteArrayInputStream(jwt.getBytes());
        final String expectedHeader = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9";
        final String expectedFooter = "hrMHJBMqZP8bQ67cSmgrc3729651JVOm3XncaKOt4EZ6KYsWA3VaXV4B7glEibBtNQRBnnw6IkbJ1Qyb5nABYBBPcUg0nB4WHLJM3eTp_rem15DQpY-sCVst3OOtSxhKa9ds9M8IvsVW1tZ2p9FKls5TXPFOD34l"
                + "GW_4n0-Zrs6m8VpiFinPwrwN6WW1CaHQhXNrgLczYRtcvICZiydxNOvvY6Mh9PYymGMYiDMF9LUh9ilADXGk-CbZGckfRegFK0Gb2d1MVE8Py2fFcVrtYCOecL_9eeHrZ1YBxGIXa7fF-eUohvj5kuj_jTxne1MIIYUlATSHIOMVLuwOGfS6"
                + "kw";

        try {
            final File file = new File("./src/test/resources/key1.pub");
            final String asciiKey = new String(Files.readAllBytes(file.toPath()));

             TestRunner runner  = setUp(DEFAULT_JWT_ATTRIBUTE_NAME, DEFAULT_JWT_ATTRIBUTE_NAME, "");
            runner.setProperty("Public Key", asciiKey);
            runner = run(runner, content);
            final MockFlowFile result = testSuccessful(runner);
            final String resultValue = new String(runner.getContentAsByteArray(result));

            // Test attributes and content
            Assert.assertEquals(expectedJson, resultValue);

            final Map<String, String> attributes = result.getAttributes();
            Assert.assertEquals(expectedHeader, attributes.get(VerifyJWT.HEADER_ATTRIBUTE));
            Assert.assertEquals(expectedFooter, attributes.get(VerifyJWT.FOOTER_ATTRIBUTE));

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJWTinAttribute() throws IOException {
        final String jwtAttributeValue = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.KYX6rBoTxRMpAmUsaFuuRv7peY1"
                + "oJXbiXy6nqvpshSh27Nri-N8aoGqb-zcJ2jN3BMRGitCT28eCAZCpKLd3UGuISmtXmqd_-OugDGWhf7OOhjzwDbxq6u0QLYaEpyJDMG0iCuekP06JsIYnkNOZIH9rGiBqnjDya4cXws3n6RqpplV44HzZ5pf_kQSWjGWay5Ii0h3w8ARDBG"
                + "fZoU4n01LNPrVv3DaUAttxId0i01FWZKtJ8-pZtG0Jon33s14BRDAZ-uPKOaBeuzKe3IF0HSSG6IUomVDbSRW-vpXRMoj2YQVl3nuaxNi4xmkvANUt-YFhhdiepRF5mOZyi0Xe1A";
        final String jwtAttributeName = "http.jwt";
        final Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("sub", "1234567890");
        expectedAttributes.put("name", "John Doe");
        expectedAttributes.put("admin", "true");
        expectedAttributes.put("iat", "1516239022");
        expectedAttributes.put("header", "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9");
        expectedAttributes.put("footer", "KYX6rBoTxRMpAmUsaFuuRv7peY1oJXbiXy6nqvpshSh27Nri-N8aoGqb-zcJ2jN3BMRGitCT28eCAZCpKLd3UGuISmtXmqd_-OugDGWhf7OOhjzwDbxq6u0QLYaEpyJDMG0iCuekP06JsIYnkNOZIH9rGiBq"
                + "njDya4cXws3n6RqpplV44HzZ5pf_kQSWjGWay5Ii0h3w8ARDBGfZoU4n01LNPrVv3DaUAttxId0i01FWZKtJ8-pZtG0Jon33s14BRDAZ-uPKOaBeuzKe3IF0HSSG6IUomVDbSRW-vpXRMoj2YQVl3nuaxNi4xmkvANUt-YFhhdiepRF5mOZ"
                + "yi0Xe1A");

        TestRunner runner = setUp(jwtAttributeName, DEFAULT_PUB_KEY_EXTENSION, PUBLIC_KEY_PATH);
        runner = run(runner, DUMMY_CONTENT, jwtAttributeName, jwtAttributeValue);

        final MockFlowFile result = testSuccessful(runner);

        final Map<String, String> resultAttributes = result.getAttributes();
        for (final Entry<String, String> expected : expectedAttributes.entrySet()) {
            Assert.assertEquals(expected.getValue(),
                    resultAttributes.get(VerifyJWT.JWT_PREFIX_ATTRIBUTE + expected.getKey()));
        }
    }

    @Test
    public void testWithIncorrectAttribute() {
        final InputStream content = new ByteArrayInputStream("Dummy content".getBytes());
        final String jwtAttributeValue = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.KYX6rBoTxRMpAmUsaFuuRv7peY1"
                + "oJXbiXy6nqvpshSh27Nri-N8aoGqb-zcJ2jN3BMRGitCT28eCAZCpKLd3UGuISmtXmqd_-OugDGWhf7OOhjzwDbxq6u0QLYaEpyJDMG0iCuekP06JsIYnkNOZIH9rGiBqnjDya4cXws3n6RqpplV44HzZ5pf_kQSWjGWay5Ii0h3w8ARDBG"
                + "fZoU4n01LNPrVv3DaUAttxId0i01FWZKtJ8-pZtG0Jon33s14BRDAZ-uPKOaBeuzKe3IF0HSSG6IUomVDbSRW-vpXRMoj2YQVl3nuaxNi4xmkvANUt-YFhhdiepRF5mOZyi0Xe1A";
        final String jwtAttributeName = "http.jwt";
        final String propertyAttribute = "jwt";

        TestRunner runner = setUp(propertyAttribute, DEFAULT_PUB_KEY_EXTENSION, PUBLIC_KEY_PATH);
        runner = run(runner, content, jwtAttributeName, jwtAttributeValue);

        Assert.assertEquals(1, runner.getFlowFilesForRelationship(VerifyJWT.FAILURE_REL).size());
        Assert.assertEquals(0, runner.getFlowFilesForRelationship(VerifyJWT.SUCCESS_REL).size());
    }

    // UTILITY FUNCTIONS BELOW
    public MockFlowFile testSuccessful(final TestRunner runner) {
        Assert.assertEquals(0, runner.getFlowFilesForRelationship(VerifyJWT.FAILURE_REL).size());
        Assert.assertEquals(1, runner.getFlowFilesForRelationship(VerifyJWT.SUCCESS_REL).size());

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(VerifyJWT.SUCCESS_REL);

        Assert.assertEquals(1, results.size());
        return results.get(0);
    }

    // Assert the processor successfully processes the content in the body of the flowfile and the outputted results are as expected
    public void testSuccessfulBody(final InputStream content, final String keyExtension, final String expectedHeader,
            final String expectedFooter) {
        TestRunner runner = setUp(DEFAULT_JWT_ATTRIBUTE_NAME, keyExtension, PUBLIC_KEY_PATH);
        runner = run(runner, content);

        final MockFlowFile result = testSuccessful(runner);
        final String resultValue = new String(runner.getContentAsByteArray(result));

        // Test attributes and content
        Assert.assertEquals(expectedJson, resultValue);

        final Map<String, String> attributes = result.getAttributes();
        Assert.assertEquals(expectedHeader, attributes.get(VerifyJWT.HEADER_ATTRIBUTE));
        Assert.assertEquals(expectedFooter, attributes.get(VerifyJWT.FOOTER_ATTRIBUTE));
    }

    // Assert the processor fails to process the content in the body of the flowfile
    public TestRunner testUnsuccessfulBody(final InputStream content, final String keyExtension) {
        TestRunner runner = setUp(DEFAULT_JWT_ATTRIBUTE_NAME, keyExtension, PUBLIC_KEY_PATH);
        runner = run(runner, content);

        Assert.assertEquals(1, runner.getFlowFilesForRelationship(VerifyJWT.FAILURE_REL).size());
        Assert.assertEquals(0, runner.getFlowFilesForRelationship(VerifyJWT.SUCCESS_REL).size());

        return runner;
    }

    // Init the processor to set up the required properties
    public TestRunner setUp(final String jwtAttributeName, final String pubKeyExtension, final String pubKeyPath) {
        // Generate a test runner to mock a processor in a flow
        final TestRunner runner = TestRunners.newTestRunner(new VerifyJWT());

        // Add properties
        runner.setProperty(VerifyJWT.PUBLIC_KEYS_PATH, pubKeyPath);
        runner.setProperty(VerifyJWT.JWT_ATTRIBUTE_NAME, jwtAttributeName);
        if (!pubKeyExtension.isEmpty()) {
            runner.setProperty(VerifyJWT.PUBLIC_KEY_EXTENSION, pubKeyExtension);
        }
        return runner;
    }

    // run the processor with content and attributes
    public TestRunner run(final TestRunner runner, final InputStream content, final String attributeName, final String attributeValue) {
        final Map<String,String> attributes = new HashMap<>();
        attributes.put(attributeName, attributeValue);

        // Add the content to the runner
        runner.enqueue(content, attributes);

        runner.assertValid();

        // Run the enqueued content
        runner.run(1);

        // All results were processed with out failure
        Assert.assertTrue(runner.isQueueEmpty());

        return runner;
    }

    public TestRunner run(final TestRunner runner, final InputStream content) {
        return run(runner, content, "", "");
    }
}