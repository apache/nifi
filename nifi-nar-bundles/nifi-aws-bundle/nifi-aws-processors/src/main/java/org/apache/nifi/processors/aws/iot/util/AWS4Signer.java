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
package org.apache.nifi.processors.aws.iot.util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class AWS4Signer {
    private static final String AWS_IOT_DATE_FORMAT = "yyyyMMdd'T'HHmmssXXX";
    private static final String AWS_IOT_SIGNED_HEADERS = "host";
    private static final String AWS_IOT_DATESTAMP_FORMAT = "yyyyMMdd";
    private static final String AWS_IOT_ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String AWS_IOT_SERVICE = "iotdevicegateway";
    private static final String AWS_IOT_PROTOCOL = "wss";
    private static final String AWS_HTTP_METHOD = "GET";
    private static final String AWS_IOT_WS_PATH = "/mqtt";

    public static String getAddress(String strRegion, String strEndpointId, AWSCredentials awsCredentials) throws Exception {
        Date dtNow = new Date();

        String strHost = strEndpointId + ".iot." + strRegion + ".amazonaws.com";
        String strAmzdate = GetUTCdatetimeAsString(dtNow, AWS_IOT_DATE_FORMAT);
        String strDatestamp = GetUTCdatetimeAsString(dtNow, AWS_IOT_DATESTAMP_FORMAT);
        String strHashedPayload = sha256("");

        String strCredentialScope =
                strDatestamp + "/" +
                        strRegion + "/" +
                        AWS_IOT_SERVICE + "/aws4_request";

        String strAmzCredential =
                awsCredentials.getAWSAccessKeyId() + "/" + strCredentialScope;

        String strCanonicalQuerystring =
                "X-Amz-Algorithm=" + AWS_IOT_ALGORITHM +
                        "&X-Amz-Credential=" + URLEncoder.encode(strAmzCredential, "UTF-8") +
                        "&X-Amz-Date=" + strAmzdate +
                        "&X-Amz-SignedHeaders=" + AWS_IOT_SIGNED_HEADERS;

        String strCanonicalHeaders = "host:" + strHost + "\n";

        String strCanonicalRequest =
                AWS_HTTP_METHOD + "\n" + AWS_IOT_WS_PATH + "\n" +
                        strCanonicalQuerystring + "\n" +
                        strCanonicalHeaders + "\n" + AWS_IOT_SIGNED_HEADERS + "\n" +
                        strHashedPayload;

        String strToSign = AWS_IOT_ALGORITHM + "\n" +
                strAmzdate + "\n" +
                strCredentialScope + "\n" +
                sha256(strCanonicalRequest);

        byte[] bSigningKey = getSignatureKey(awsCredentials.getAWSSecretKey(), strDatestamp, strRegion, AWS_IOT_SERVICE);
        byte[] bSignature = HmacSHA256(strToSign, bSigningKey);

        strCanonicalQuerystring += "&X-Amz-Signature=" + bin2hex(bSignature);

        if (awsCredentials instanceof AWSSessionCredentials) {
            String strSessionToken = ((AWSSessionCredentials) awsCredentials).getSessionToken();
            strCanonicalQuerystring += "&X-Amz-Security-Token=" + URLEncoder.encode(strSessionToken, "UTF-8");
        }

        return AWS_IOT_PROTOCOL + "://" + strHost + AWS_IOT_WS_PATH + "?" + strCanonicalQuerystring;
    }

    public static String sha256(String data) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.reset();
        return bin2hex(digest.digest(data.getBytes("UTF-8")));
    }

    private static String bin2hex(byte[] data) {
        return String.format("%0" + (data.length * 2) + "X", new BigInteger(1, data)).toLowerCase();
    }

    private static String GetUTCdatetimeAsString(Date dt, String strFormat)
    {
        final SimpleDateFormat sdf = new SimpleDateFormat(strFormat);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(dt);
    }

    private static byte[] HmacSHA256(String data, byte[] key) throws Exception  {
        String algorithm="HmacSHA256";
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(data.getBytes("UTF8"));
    }

    private static byte[] getSignatureKey(String key, String dateStamp, String regionName, String serviceName) throws Exception  {
        byte[] kSecret = ("AWS4" + key).getBytes("UTF8");
        byte[] kDate    = HmacSHA256(dateStamp, kSecret);
        byte[] kRegion  = HmacSHA256(regionName, kDate);
        byte[] kService = HmacSHA256(serviceName, kRegion);
        return HmacSHA256("aws4_request", kService);
    }
}
