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
package org.apache.nifi.web.security.x509.ocsp;

/**
 *
 */
public class OcspStatus {

    public enum ResponseStatus {

        Successful,
        MalformedRequest,
        InternalError,
        TryLater,
        SignatureRequired,
        Unauthorized,
        Unknown
    }

    public enum VerificationStatus {

        Unknown,
        Verified,
        Unverified
    };

    public enum ValidationStatus {

        Unknown,
        Good,
        Revoked
    };

    private ResponseStatus responseStatus;
    private VerificationStatus verificationStatus;
    private ValidationStatus validationStatus;

    public ResponseStatus getResponseStatus() {
        return responseStatus;
    }

    public void setResponseStatus(ResponseStatus responseStatus) {
        this.responseStatus = responseStatus;
    }

    public VerificationStatus getVerificationStatus() {
        return verificationStatus;
    }

    public void setVerificationStatus(VerificationStatus verificationStatus) {
        this.verificationStatus = verificationStatus;
    }

    public ValidationStatus getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(ValidationStatus validationStatus) {
        this.validationStatus = validationStatus;
    }

    @Override
    public String toString() {
        return String.format("Request (%s) Verification (%s) Validation (%s)", responseStatus, verificationStatus, validationStatus);
    }

}
