package org.apache.nifi.jasn1;

import com.beanit.jasn1.ber.types.BerType;
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
