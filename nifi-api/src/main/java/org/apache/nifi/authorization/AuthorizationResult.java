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
 * Represents a decision whether authorization is granted.
 */
public class AuthorizationResult {

    private enum Result {
        Approved,
        Denied,
        ResourceNotFound
    }

    private static final AuthorizationResult APPROVED = new AuthorizationResult(Result.Approved, null);
    private static final AuthorizationResult RESOURCE_NOT_FOUND = new AuthorizationResult(Result.ResourceNotFound, null);

    private final Result result;
    private final String explanation;

    /**
     * Creates a new AuthorizationResult with the specified result and explanation.
     *
     * @param result of the authorization
     * @param explanation for the authorization attempt
     */
    private AuthorizationResult(Result result, String explanation) {
        if (Result.Denied.equals(result) && explanation == null) {
            throw new IllegalArgumentException("An explanation is required when the authorization request is denied.");
        }

        this.result = result;
        this.explanation = explanation;
    }

    /**
     * @return Whether or not the request is approved
     */
    public Result getResult() {
        return result;
    }

    /**
     * @return If the request is denied, the reason why. Null otherwise
     */
    public String getExplanation() {
        return explanation;
    }

    /**
     * @return a new approved AuthorizationResult
     */
    public static AuthorizationResult approved() {
        return APPROVED;
    }

    /**
     * Resource not found will indicate that there are no specific authorization rules for this resource.
     * @return a new resource not found AuthorizationResult
     */
    public static AuthorizationResult resourceNotFound() {
        return RESOURCE_NOT_FOUND;
    }

    /**
     * Creates a new denied AuthorizationResult with a message indicating 'Access is denied'.
     *
     * @return a new denied AuthorizationResult
     */
    public static AuthorizationResult denied() {
        return denied("Access is denied");
    }

    /**
     * Creates a new denied AuthorizationResult with the specified explanation.
     *
     * @param explanation for why it was denied
     * @return a new denied AuthorizationResult with the specified explanation
     * @throws IllegalArgumentException if explanation is null
     */
    public static AuthorizationResult denied(String explanation) {
        return new AuthorizationResult(Result.Denied, explanation);
    }
}
