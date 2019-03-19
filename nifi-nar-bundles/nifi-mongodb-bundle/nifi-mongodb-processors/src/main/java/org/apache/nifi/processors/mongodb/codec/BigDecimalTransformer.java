/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb.codec;

import org.bson.Transformer;

import java.math.BigDecimal;

//credits: https://gist.github.com/squarepegsys/9a97f7c70337e7c5e006a436acd8a729
public class BigDecimalTransformer implements Transformer {
    @Override
    public Object transform(Object objectToTransform) {
        BigDecimal value = (BigDecimal) objectToTransform;
        return value.toString();
    }
}
