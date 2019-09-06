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
package org.apache.nifi.jasn1.util;

import com.beanit.asn1bean.ber.ReverseByteArrayOutputStream;
import com.beanit.asn1bean.ber.types.BerType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class JASN1DataWriter {
    public static void write(BerType berObject, String outputFile) {
        final File file = new File(outputFile);

        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (
                final FileOutputStream fileOutputStream = new FileOutputStream(file);
                final ReverseByteArrayOutputStream outputStream = new ReverseByteArrayOutputStream(1024);
        ) {
            final int encodeOffset = berObject.encode(outputStream);

            fileOutputStream.write(outputStream.getArray(), 0, encodeOffset);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
