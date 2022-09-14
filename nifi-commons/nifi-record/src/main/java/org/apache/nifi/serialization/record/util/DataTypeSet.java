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

package org.apache.nifi.serialization.record.util;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A container class, to which multiple DataTypes can be added, such that
 * adding any two types where one is more narrow than the other will result
 * in combining the two types into the wider type.
 */
public class DataTypeSet {
    private final List<DataType> types = new ArrayList<>();

    /**
     * Adds the given data type to the set of types to consider
     * @param dataType the data type to add
     */
    public void add(final DataType dataType) {
        if (dataType == null) {
            return;
        }

        if (dataType.getFieldType() == RecordFieldType.CHOICE) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            choiceDataType.getPossibleSubTypes().forEach(this::add);
            return;
        }

        if (types.contains(dataType)) {
            return;
        }

        DataType toRemove = null;
        DataType toAdd = null;
        for (final DataType currentType : types) {
            final Optional<DataType> widerType = DataTypeUtils.getWiderType(currentType, dataType);
            if (widerType.isPresent()) {
                toRemove = currentType;
                toAdd = widerType.get();
            }
        }

        if (toRemove != null) {
            types.remove(toRemove);
        }

        types.add( toAdd == null ? dataType : toAdd );
    }

    /**
     * @return the combined types
     */
    public List<DataType> getTypes() {
        return new ArrayList<>(types);
    }
}
