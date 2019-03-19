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

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.math.BigDecimal;

//credits: https://gist.github.com/squarepegsys/9a97f7c70337e7c5e006a436acd8a729
public class BigDecimalCodec implements Codec<BigDecimal> {
    @Override
    public void encode(final BsonWriter writer, final BigDecimal value, final EncoderContext encoderContext) {
        writer.writeString(value.toString());
    }

    @Override
    public BigDecimal decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new BigDecimal(reader.readString());
    }

    @Override
    public Class<BigDecimal> getEncoderClass() {
        return BigDecimal.class;
    }
}
