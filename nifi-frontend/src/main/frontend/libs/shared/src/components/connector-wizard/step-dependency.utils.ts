/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ConfigurationStepConfiguration, ConfigurationStepDependency, ConnectorPropertyFormValue } from '../../types';
import { fromValueReference } from '../../services/value-reference.helper';

/**
 * Get the current value of a property from a step.
 * Priority: unsavedStepValues > propertyValues from stepConfiguration
 *
 * @param stepName The step name to get the property value from
 * @param propertyName The property name to get the value for
 * @param stepConfigurations All step configurations (saved values)
 * @param unsavedStepValues Unsaved form values per step
 * @returns The property value or undefined if not found
 */
function getPropertyValue(
    stepName: string,
    propertyName: string,
    stepConfigurations: { [stepName: string]: ConfigurationStepConfiguration },
    unsavedStepValues: Record<string, Record<string, ConnectorPropertyFormValue>>
): ConnectorPropertyFormValue | undefined {
    // Check unsaved values first (form state takes priority)
    const unsavedValue = unsavedStepValues[stepName]?.[propertyName];
    if (unsavedValue !== undefined) {
        return unsavedValue;
    }

    // Fall back to saved configuration
    const stepConfig = stepConfigurations[stepName];
    if (!stepConfig) {
        return undefined;
    }

    // Search property groups for the property value
    for (const group of stepConfig.propertyGroupConfigurations || []) {
        if (group.propertyValues?.[propertyName] !== undefined) {
            const propertyDescriptor = group.propertyDescriptors?.[propertyName];
            return fromValueReference(group.propertyValues[propertyName], propertyDescriptor?.type);
        }
    }

    return undefined;
}

/**
 * Evaluate if a single step dependency is satisfied.
 * A dependency on a hidden step is NOT satisfied (transitive hiding).
 *
 * This logic aligns with how property dependencies work in the property table:
 * - If dependentValues is empty/undefined, any non-empty value satisfies the dependency
 * - If dependentValues has values, the current value must be in the list
 *
 * @param dependency The step dependency to evaluate
 * @param stepConfigurations All step configurations (saved values)
 * @param unsavedStepValues Unsaved form values per step
 * @param visibleSteps Set of step names that are currently visible (for transitive check)
 * @returns boolean indicating if the dependency is satisfied
 */
export function isStepDependencySatisfied(
    dependency: ConfigurationStepDependency,
    stepConfigurations: { [stepName: string]: ConfigurationStepConfiguration },
    unsavedStepValues: Record<string, Record<string, ConnectorPropertyFormValue>>,
    visibleSteps: Set<string>
): boolean {
    const { stepName, propertyName, dependentValues } = dependency;

    // If the step we depend on is not visible, this dependency is not met (transitive hiding)
    if (!visibleSteps.has(stepName)) {
        return false;
    }

    // Get the current property value
    const value = getPropertyValue(stepName, propertyName, stepConfigurations, unsavedStepValues);

    // Evaluate based on dependentValues
    if (!dependentValues || dependentValues.length === 0) {
        // No specific values required - any non-empty value satisfies
        return value !== null && value !== undefined && value !== '';
    } else {
        // Value must be in the allowed list
        return value != null && dependentValues.includes(String(value));
    }
}

/**
 * Get visible step names based on dependency evaluation.
 * Processes steps in order since dependencies are forward-only (a step can only
 * depend on properties from previous steps).
 *
 * Steps without dependencies or without a loaded configuration are always visible.
 * Hidden steps still have their configurations preserved in state.
 *
 * @param stepNames All step names in order
 * @param stepConfigurations Step configurations (may be partially loaded)
 * @param unsavedStepValues Unsaved form values per step
 * @returns Array of visible step names (preserves original order)
 */
export function getVisibleStepNames(
    stepNames: string[],
    stepConfigurations: { [stepName: string]: ConfigurationStepConfiguration },
    unsavedStepValues: Record<string, Record<string, ConnectorPropertyFormValue>>
): string[] {
    const visibleSteps = new Set<string>();

    for (const stepName of stepNames) {
        const stepConfig = stepConfigurations[stepName];

        // If no config loaded yet or no dependencies, step is visible
        if (!stepConfig || !stepConfig.dependencies || stepConfig.dependencies.length === 0) {
            visibleSteps.add(stepName);
            continue;
        }

        // Check all dependencies (all must be satisfied for step to be visible)
        const allDependenciesMet = stepConfig.dependencies.every((dep) =>
            isStepDependencySatisfied(dep, stepConfigurations, unsavedStepValues, visibleSteps)
        );

        if (allDependenciesMet) {
            visibleSteps.add(stepName);
        }
    }

    // Return in original order, filtering to only visible steps
    return stepNames.filter((name) => visibleSteps.has(name));
}
