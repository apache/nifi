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
package org.apache.nifi.jasn1;

import com.beanit.asn1bean.ber.types.BerType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.nifi.jasn1.JASN1Utils.invokeGetter;

public class StandardRecordModelIteratorProvider implements RecordModelIteratorProvider {

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<BerType> iterator(InputStream inputStream, ComponentLog logger, Class<? extends BerType> rootClass, String recordField, Field seqOfField) {
        final BerType model;
        try {
            model = rootClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to instantiate " + rootClass.getCanonicalName(), e);
        }

        try {
            final int decode = model.decode(inputStream);
            logger.debug("Decoded {} bytes into {}", new Object[]{decode, model.getClass()});
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode " + rootClass.getCanonicalName(), e);
        }

        final List<BerType> recordModels;
        if (StringUtils.isEmpty(recordField)) {
            recordModels = Collections.singletonList(model);
        } else {
            try {
                final Method recordModelGetter = rootClass.getMethod(JASN1Utils.toGetterMethod(recordField));
                final BerType readPointModel = (BerType) recordModelGetter.invoke(model);
                if (seqOfField != null) {
                    final Class seqOf = JASN1Utils.getSeqOfElementType(seqOfField);
                    recordModels = (List<BerType>) invokeGetter(readPointModel, JASN1Utils.toGetterMethod(seqOf.getSimpleName()));
                } else {
                    recordModels = Collections.singletonList(readPointModel);
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to get record models due to " + e, e);
            }
        }

        return recordModels.iterator();
    }
}
