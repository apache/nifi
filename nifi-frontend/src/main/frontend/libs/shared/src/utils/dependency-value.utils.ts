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

import { ConnectorPropertyFormValue } from '../types';

/**
 * Evaluates whether a dependency condition is met by a property's current value.
 *
 * The API represents dependent values as strings, while live form values can be native
 * booleans. Dependency matching remains case-sensitive to match backend dependency evaluators.
 *
 * @param value The dependent property's current value
 * @param dependentValues Values that satisfy the dependency; empty means any non-empty value
 * @returns whether the dependency condition is met
 */
export function isDependencyValueSatisfied(
    value: ConnectorPropertyFormValue | undefined,
    dependentValues: string[] | undefined | null
): boolean {
    if (!dependentValues || dependentValues.length === 0) {
        return value !== null && value !== undefined && value !== '';
    }

    return value !== null && value !== undefined && dependentValues.includes(String(value));
}
