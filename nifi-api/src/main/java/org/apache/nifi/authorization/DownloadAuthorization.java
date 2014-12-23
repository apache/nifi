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
     * Creates a new DownloadAuthorization with the specified result and explanation.
     * 
     * @param result
     * @param explanation 
     */
    private DownloadAuthorization(Result result, String explanation) {
        if (Result.Denied.equals(result) && explanation == null) {
            throw new IllegalArgumentException("An explanation is required when the download request is denied.");
        }

        this.result = result;
        this.explanation = explanation;
    }

    /**
     * Whether or not the download request is approved.
     * 
     * @return 
     */
    public boolean isApproved() {
        return Result.Approved.equals(result);
    }

    /**
     * If the download request is denied, the reason why. Null otherwise.
     * 
     * @return 
     */
    public String getExplanation() {
        return explanation;
    }

    /**
     * Creates a new approved DownloadAuthorization.
     * 
     * @return 
     */
    public static DownloadAuthorization approved() {
        return APPROVED;
    }

    /**
     * Creates a new denied DownloadAuthorization with the specified explanation.
     * 
     * @param explanation
     * @return 
     * @throws IllegalArgumentException     if explanation is null
     */
    public static DownloadAuthorization denied(String explanation) {
        return new DownloadAuthorization(Result.Denied, explanation);
    }
}
