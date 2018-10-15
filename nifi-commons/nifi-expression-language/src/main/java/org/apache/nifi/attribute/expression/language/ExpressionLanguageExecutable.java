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
package org.apache.nifi.attribute.expression.language;


public interface ExpressionLanguageExecutable {
    /**
     * Default execute signature implementation.
     * Any new UDF that implements this interface is free to override this method for default behavior.
     * New methods can be added to implementation. All methods should have the same method name "execute".
     * Method overloading is allowed. Any new method should'nt be "void". toString() will be called on returned object.
     * In case the EL framework cannot find specific method for given attributes,
     * default "execute" method will be called.
     *
     * @param args parameters to be passed to this method.
     * @return default value for method invocation (null). Implementation can override this method.
     */
    default String execute(Object... args) {
        return null;
    }
}
