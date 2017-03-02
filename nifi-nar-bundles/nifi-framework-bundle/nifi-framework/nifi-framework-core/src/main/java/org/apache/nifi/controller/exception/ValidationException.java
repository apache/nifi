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
package org.apache.nifi.controller.exception;

import java.util.Collections;
import java.util.List;

public class ValidationException extends RuntimeException {

    private static final long serialVersionUID = 198023479823479L;

    private final List<String> errors;

    public ValidationException(final List<String> errors) {
        this.errors = errors;
    }

    public List<String> getValidationErrors() {
        return Collections.unmodifiableList(errors);
    }

    @Override
    public String getLocalizedMessage() {
        final StringBuilder sb = new StringBuilder();
        sb.append(errors.size()).append(" validation error");
        if (errors.size() == 1) {
            sb.append(": ").append(errors.get(0));
        } else {
            sb.append("s");
        }
        return sb.toString();
    }
}
