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
package org.apache.nifi.processor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum DataUnit {

    /**
     * Bytes
     */
    B {
                @Override
                public double toB(double value) {
                    return value;
                }

                @Override
                public double toKB(double value) {
                    return value / POWERS[1];
                }

                @Override
                public double toMB(double value) {
                    return value / POWERS[2];
                }

                @Override
                public double toGB(double value) {
                    return value / POWERS[3];
                }

                @Override
                public double toTB(double value) {
                    return value / POWERS[4];
                }

                @Override
                public double convert(double sourceSize, DataUnit sourceUnit) {
                    return sourceUnit.toB(sourceSize);
                }
            },
    /**
     * Kilobytes
     */
    KB {
                @Override
                public double toB(double value) {
                    return value * POWERS[1];
                }

                @Override
                public double toKB(double value) {
                    return value;
                }

                @Override
                public double toMB(double value) {
                    return value / POWERS[1];
                }

                @Override
                public double toGB(double value) {
                    return value / POWERS[2];
                }

                @Override
                public double toTB(double value) {
                    return value / POWERS[3];
                }

                @Override
                public double convert(double sourceSize, DataUnit sourceUnit) {
                    return sourceUnit.toKB(sourceSize);
                }
            },
    /**
     * Megabytes
     */
    MB {
                @Override
                public double toB(double value) {
                    return value * POWERS[2];
                }

                @Override
                public double toKB(double value) {
                    return value * POWERS[1];
                }

                @Override
                public double toMB(double value) {
                    return value;
                }

                @Override
                public double toGB(double value) {
                    return value / POWERS[1];
                }

                @Override
                public double toTB(double value) {
                    return value / POWERS[2];
                }

                @Override
                public double convert(double sourceSize, DataUnit sourceUnit) {
                    return sourceUnit.toMB(sourceSize);
                }
            },
    /**
     * Gigabytes
     */
    GB {
                @Override
                public double toB(double value) {
                    return value * POWERS[3];
                }

                @Override
                public double toKB(double value) {
                    return value * POWERS[2];
                }

                @Override
                public double toMB(double value) {
                    return value * POWERS[1];
                }

                @Override
                public double toGB(double value) {
                    return value;
                }

                @Override
                public double toTB(double value) {
                    return value / POWERS[1];
                }

                @Override
                public double convert(double sourceSize, DataUnit sourceUnit) {
                    return sourceUnit.toGB(sourceSize);
                }
            },
    /**
     * Terabytes
     */
    TB {
                @Override
                public double toB(double value) {
                    return value * POWERS[4];
                }

                @Override
                public double toKB(double value) {
                    return value * POWERS[3];
                }

                @Override
                public double toMB(double value) {
                    return value * POWERS[2];
                }

                @Override
                public double toGB(double value) {
                    return value * POWERS[1];
                }

                @Override
                public double toTB(double value) {
                    return value;
                }

                @Override
                public double convert(double sourceSize, DataUnit sourceUnit) {
                    return sourceUnit.toTB(sourceSize);
                }
            };

    public double convert(final double sourceSize, final DataUnit sourceUnit) {
        throw new AbstractMethodError();
    }

    public double toB(double size) {
        throw new AbstractMethodError();
    }

    public double toKB(double size) {
        throw new AbstractMethodError();
    }

    public double toMB(double size) {
        throw new AbstractMethodError();
    }

    public double toGB(double size) {
        throw new AbstractMethodError();
    }

    public double toTB(double size) {
        throw new AbstractMethodError();
    }

    public static final double[] POWERS = {1,
        1024D,
        1024 * 1024D,
        1024 * 1024 * 1024D,
        1024 * 1024 * 1024 * 1024D};

    public static final String DATA_SIZE_REGEX = "(\\d+(?:\\.\\d+)?)\\s*(B|KB|MB|GB|TB)";
    public static final Pattern DATA_SIZE_PATTERN = Pattern.compile(DATA_SIZE_REGEX);

    public static Double parseDataSize(final String value, final DataUnit units) {
        if (value == null) {
            return null;
        }

        final Matcher matcher = DATA_SIZE_PATTERN.matcher(value.toUpperCase());
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid data size: " + value);
        }

        final String sizeValue = matcher.group(1);
        final String unitValue = matcher.group(2);

        final DataUnit sourceUnit = DataUnit.valueOf(unitValue);
        final double size = Double.parseDouble(sizeValue);
        return units.convert(size, sourceUnit);
    }
}
