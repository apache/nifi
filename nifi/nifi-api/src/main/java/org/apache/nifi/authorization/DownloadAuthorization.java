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
package org.apache.nifi.authorization;

/**
 * Represents a decision whether authorization is granted to download content.
 */
public class DownloadAuthorization {

    private static enum Result {

        Approved,
        Denied;
    };

    private static final DownloadAuthorization APPROVED = new DownloadAuthorization(Result.Approved, null);

    private final Result result;
    private final String explanation;

    /**
     * Creates a new DownloadAuthorization with the specified result and
     * explanation.
     *
     * @param result of the authorization
     * @param explanation for the authorization attempt
     */
    private DownloadAuthorization(Result result, String explanation) {
        if (Result.Denied.equals(result) && explanation == null) {
            throw new IllegalArgumentException("An explanation is required when the download request is denied.");
        }

        this.result = result;
        this.explanation = explanation;
    }

    /**
     * @return Whether or not the download request is approved
     */
    public boolean isApproved() {
        return Result.Approved.equals(result);
    }

    /**
     * @return If the download request is denied, the reason why. Null otherwise
     */
    public String getExplanation() {
        return explanation;
    }

    /**
     * @return a new approved DownloadAuthorization
     */
    public static DownloadAuthorization approved() {
        return APPROVED;
    }

    /**
     * Creates a new denied DownloadAuthorization with the specified
     * explanation.
     *
     * @param explanation for why it was denied
     * @return a new denied DownloadAuthorization with the specified explanation
     * @throws IllegalArgumentException if explanation is null
     */
    public static DownloadAuthorization denied(String explanation) {
        return new DownloadAuthorization(Result.Denied, explanation);
    }
}
