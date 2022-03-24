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
package org.apache.nifi.attribute.expression.language.exception;

public class AttributeExpressionLanguageParsingException extends AttributeExpressionLanguageException {

    private static final long serialVersionUID = 7422163230677064726L;

    public AttributeExpressionLanguageParsingException(final String explanation) {
        super(explanation);
    }

    public AttributeExpressionLanguageParsingException(final String explanation, final Throwable t) {
        super(explanation, t);
    }

    public AttributeExpressionLanguageParsingException(final Throwable t) {
        super(t);
    }
}
