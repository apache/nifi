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
package org.apache.nifi.processors.hudi;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.AutoRecordGenWrapperAvroKeyGenerator;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.GlobalAvroDeleteKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.hudi.config.HoodieWriteConfig.KEYGENERATOR_CLASS_NAME;
import static org.apache.hudi.config.HoodieWriteConfig.KEYGENERATOR_TYPE;
import static org.apache.hudi.keygen.KeyGenUtils.inferKeyGeneratorType;

/**
 * Factory help to create {@link org.apache.hudi.keygen.KeyGenerator}.
 */
public class HudiKeyGeneratorFactory {
    private static final Logger logger = LoggerFactory.getLogger(HudiKeyGeneratorFactory.class);

    private static final Map<String, String> MAP_TO_COMMON_KEY_GENERATOR = new HashMap<>();

    static {
        MAP_TO_COMMON_KEY_GENERATOR.put("org.apache.hudi.keygen.ComplexKeyGenerator", "org.apache.hudi.keygen.ComplexAvroKeyGenerator");
        MAP_TO_COMMON_KEY_GENERATOR.put("org.apache.hudi.keygen.CustomKeyGenerator", "org.apache.hudi.keygen.CustomAvroKeyGenerator");
        MAP_TO_COMMON_KEY_GENERATOR.put("org.apache.hudi.keygen.GlobalDeleteKeyGenerator", "org.apache.hudi.keygen.GlobalAvroDeleteKeyGenerator");
        MAP_TO_COMMON_KEY_GENERATOR.put("org.apache.hudi.keygen.NonpartitionedKeyGenerator", "org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator");
        MAP_TO_COMMON_KEY_GENERATOR.put("org.apache.hudi.keygen.SimpleKeyGenerator", "org.apache.hudi.keygen.SimpleAvroKeyGenerator");
        MAP_TO_COMMON_KEY_GENERATOR.put("org.apache.hudi.keygen.TimestampBasedKeyGenerator", "org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator");
    }

    /**
     * Convert SparkKeyGeneratorInterface implement to hoodie-common KeyGenerator.
     */
    public static String convertToCommonKeyGenerator(String keyGeneratorClassName) {
        return MAP_TO_COMMON_KEY_GENERATOR.getOrDefault(keyGeneratorClassName, keyGeneratorClassName);
    }

    public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
        final String keyGeneratorClass = getKeyGeneratorClassName(props);
        final boolean autoRecordKeyGen = KeyGenUtils.enableAutoGenerateRecordKeys(props);
        try {
            final KeyGenerator keyGenerator = (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
            if (autoRecordKeyGen) {
                return new AutoRecordGenWrapperAvroKeyGenerator(props, (BaseKeyGenerator) keyGenerator);
            } else {
                return keyGenerator;
            }
        } catch (Throwable e) {
            throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
        }
    }

    private static String getKeyGeneratorClassName(TypedProperties props) {
        String keyGeneratorClass = props.getString(KEYGENERATOR_CLASS_NAME.key(), null);

        if (StringUtils.isNullOrEmpty(keyGeneratorClass)) {
            final String keyGeneratorType = props.getString(KEYGENERATOR_TYPE.key(), null);
            KeyGeneratorType keyGeneratorTypeEnum;
            if (StringUtils.isNullOrEmpty(keyGeneratorType)) {
                keyGeneratorTypeEnum = inferKeyGeneratorTypeFromWriteConfig(props);
                logger.info("The value of {} is empty; inferred to be {}", KEYGENERATOR_TYPE.key(), keyGeneratorTypeEnum);
            } else {
                try {
                    keyGeneratorTypeEnum = KeyGeneratorType.valueOf(keyGeneratorType.toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
                }
            }
            keyGeneratorClass = getKeyGeneratorClassNameFromType(keyGeneratorTypeEnum);
        }
        return keyGeneratorClass;
    }

    /**
     * Get a Key generator based on the provided type.
     *
     * @param type {@link KeyGeneratorType} enum.
     * @return The key generator class name for the {@link KeyGeneratorType}.
     */
    private static String getKeyGeneratorClassNameFromType(KeyGeneratorType type) {
        switch (type) {
            case SIMPLE:
                return SimpleAvroKeyGenerator.class.getName();
            case COMPLEX:
                return ComplexAvroKeyGenerator.class.getName();
            case TIMESTAMP:
                return TimestampBasedAvroKeyGenerator.class.getName();
            case CUSTOM:
                return CustomAvroKeyGenerator.class.getName();
            case NON_PARTITION:
                return NonpartitionedAvroKeyGenerator.class.getName();
            case GLOBAL_DELETE:
                return GlobalAvroDeleteKeyGenerator.class.getName();
            default:
                throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + type);
        }
    }

    /**
     * Infers the key generator type based on the record key and partition fields.
     * If neither of the record key and partition fields are set, the default type is returned.
     *
     * @param props Properties from the write config.
     * @return Inferred key generator type.
     */
    public static KeyGeneratorType inferKeyGeneratorTypeFromWriteConfig(TypedProperties props) {
        final String partitionFields = props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), null);
        final String recordsKeyFields = props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), null);
        return inferKeyGeneratorType(Option.ofNullable(recordsKeyFields), partitionFields);
    }
}
