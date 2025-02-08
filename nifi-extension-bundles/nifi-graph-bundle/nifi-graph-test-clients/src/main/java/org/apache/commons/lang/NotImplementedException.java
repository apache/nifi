
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.lang;

/**
 * Stub class due to JanusGraph usage of Commons Lang 2.6 which is a vulnerable version
 * Once janusgraph no longer depends on this vulnerable commons lang lib this class can be
 * deleted.
 */
public class NotImplementedException extends UnsupportedOperationException {

    private final String code;

    public NotImplementedException() {
        this.code = null;
    }

    public NotImplementedException(final String message) {
        this(message, (String) null);
    }

    public NotImplementedException(final String message, final String code) {
        super(message);
        this.code = code;
    }

    public NotImplementedException(final String message, final Throwable cause) {
        this(message, cause, null);
    }

    public NotImplementedException(final String message, final Throwable cause, final String code) {
        super(message, cause);
        this.code = code;
    }

    public NotImplementedException(final Throwable cause) {
        this(cause, null);
    }

    public NotImplementedException(final Throwable cause, final String code) {
        super(cause);
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }
}
