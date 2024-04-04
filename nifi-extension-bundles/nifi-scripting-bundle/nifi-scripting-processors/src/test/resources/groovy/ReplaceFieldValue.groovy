/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License') you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

def replace(rec) {
    rec.toMap().each { k, v ->
        // If the field value is equal to the attribute 'Value to Replace', then set the
        // field value to the 'Replacement Value' attribute.
        if (v?.toString()?.equals(attributes['Value to Replace'])) {
            rec.setValue(k, attributes['Replacement Value'])
        }

        // Call Recursively if the value is a Record
        if (v instanceof org.apache.nifi.serialization.record.Record) {
            replace(v)
        }
    }
}

replace(record)
return record
