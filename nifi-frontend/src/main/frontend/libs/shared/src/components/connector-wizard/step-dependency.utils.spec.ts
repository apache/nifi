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

import { ConfigurationStepConfiguration, ConnectorPropertyDescriptor, ConnectorValueReference } from '../../types';
import { getVisibleStepNames, isStepDependencySatisfied } from './step-dependency.utils';

describe('Step Dependency Utils', () => {
    function createStepConfig(
        stepName: string,
        dependencies: { stepName: string; propertyName: string; dependentValues?: string[] }[] = [],
        propertyValues: { [groupName: string]: { [propertyName: string]: string } } = {}
    ): ConfigurationStepConfiguration {
        const propertyGroupConfigurations = Object.entries(propertyValues).map(([groupName, values]) => ({
            propertyGroupName: groupName,
            propertyDescriptors: Object.keys(values).reduce(
                (acc, propName) => {
                    acc[propName] = {
                        name: propName,
                        type: 'STRING' as const,
                        required: false
                    };
                    return acc;
                },
                {} as Record<string, ConnectorPropertyDescriptor>
            ),
            propertyValues: Object.entries(values).reduce(
                (acc, [propName, value]) => {
                    acc[propName] = { value, valueType: 'STRING_LITERAL' as const };
                    return acc;
                },
                {} as Record<string, ConnectorValueReference>
            )
        }));

        return {
            configurationStepName: stepName,
            dependencies,
            propertyGroupConfigurations
        };
    }

    describe('isStepDependencySatisfied', () => {
        it('should return false if the dependent step is not visible', () => {
            const dependency = { stepName: 'Step1', propertyName: 'prop1' };
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { prop1: 'value1' } })
            };
            const visibleSteps = new Set<string>();

            expect(isStepDependencySatisfied(dependency, stepConfigurations, {}, visibleSteps)).toBe(false);
        });

        it('should return true if property has any value when no dependentValues specified', () => {
            const dependency = { stepName: 'Step1', propertyName: 'prop1' };
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { prop1: 'any-value' } })
            };
            const visibleSteps = new Set(['Step1']);

            expect(isStepDependencySatisfied(dependency, stepConfigurations, {}, visibleSteps)).toBe(true);
        });

        it('should return false if property is empty when no dependentValues specified', () => {
            const dependency = { stepName: 'Step1', propertyName: 'prop1' };
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { prop1: '' } })
            };
            const visibleSteps = new Set(['Step1']);

            expect(isStepDependencySatisfied(dependency, stepConfigurations, {}, visibleSteps)).toBe(false);
        });

        it('should return true if property value matches one of dependentValues', () => {
            const dependency = { stepName: 'Step1', propertyName: 'prop1', dependentValues: ['value1', 'value2'] };
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { prop1: 'value1' } })
            };
            const visibleSteps = new Set(['Step1']);

            expect(isStepDependencySatisfied(dependency, stepConfigurations, {}, visibleSteps)).toBe(true);
        });

        it('should return false if property value does not match dependentValues', () => {
            const dependency = { stepName: 'Step1', propertyName: 'prop1', dependentValues: ['value1', 'value2'] };
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { prop1: 'value3' } })
            };
            const visibleSteps = new Set(['Step1']);

            expect(isStepDependencySatisfied(dependency, stepConfigurations, {}, visibleSteps)).toBe(false);
        });

        it('should use unsaved values over saved values', () => {
            const dependency = { stepName: 'Step1', propertyName: 'prop1', dependentValues: ['unsaved-value'] };
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { prop1: 'saved-value' } })
            };
            const unsavedValues = { Step1: { prop1: 'unsaved-value' } };
            const visibleSteps = new Set(['Step1']);

            expect(isStepDependencySatisfied(dependency, stepConfigurations, unsavedValues, visibleSteps)).toBe(true);
        });

        it('should return false if step configuration does not exist', () => {
            const dependency = { stepName: 'NonExistent', propertyName: 'prop1' };
            const visibleSteps = new Set(['NonExistent']);

            expect(isStepDependencySatisfied(dependency, {}, {}, visibleSteps)).toBe(false);
        });
    });

    describe('getVisibleStepNames', () => {
        it('should return all steps when none have dependencies', () => {
            const stepNames = ['Step1', 'Step2', 'Step3'];
            const stepConfigurations = {
                Step1: createStepConfig('Step1'),
                Step2: createStepConfig('Step2'),
                Step3: createStepConfig('Step3')
            };

            expect(getVisibleStepNames(stepNames, stepConfigurations, {})).toEqual(['Step1', 'Step2', 'Step3']);
        });

        it('should show step when its dependency is satisfied', () => {
            const stepNames = ['Step1', 'Step2'];
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { connectionType: 'advanced' } }),
                Step2: createStepConfig('Step2', [
                    { stepName: 'Step1', propertyName: 'connectionType', dependentValues: ['advanced'] }
                ])
            };

            expect(getVisibleStepNames(stepNames, stepConfigurations, {})).toEqual(['Step1', 'Step2']);
        });

        it('should hide step when its dependency is not satisfied', () => {
            const stepNames = ['Step1', 'Step2'];
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { connectionType: 'basic' } }),
                Step2: createStepConfig('Step2', [
                    { stepName: 'Step1', propertyName: 'connectionType', dependentValues: ['advanced'] }
                ])
            };

            expect(getVisibleStepNames(stepNames, stepConfigurations, {})).toEqual(['Step1']);
        });

        it('should handle transitive dependencies (Step3 -> Step2 -> Step1)', () => {
            const stepNames = ['Step1', 'Step2', 'Step3'];
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { enableAdvanced: 'false' } }),
                Step2: createStepConfig('Step2', [
                    { stepName: 'Step1', propertyName: 'enableAdvanced', dependentValues: ['true'] }
                ]),
                Step3: createStepConfig('Step3', [{ stepName: 'Step2', propertyName: 'advancedOption' }])
            };

            expect(getVisibleStepNames(stepNames, stepConfigurations, {})).toEqual(['Step1']);
        });

        it('should show step when config is not yet loaded', () => {
            const stepNames = ['Step1', 'Step2'];
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { prop1: 'value' } })
            };

            expect(getVisibleStepNames(stepNames, stepConfigurations, {})).toEqual(['Step1', 'Step2']);
        });

        it('should react to unsaved form values for immediate visibility updates', () => {
            const stepNames = ['Step1', 'Step2'];
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { connectionType: 'basic' } }),
                Step2: createStepConfig('Step2', [
                    { stepName: 'Step1', propertyName: 'connectionType', dependentValues: ['advanced'] }
                ])
            };
            const unsavedValues = { Step1: { connectionType: 'advanced' } };

            expect(getVisibleStepNames(stepNames, stepConfigurations, unsavedValues)).toEqual(['Step1', 'Step2']);
        });

        it('should require all dependencies to be satisfied', () => {
            const stepNames = ['Step1', 'Step2'];
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { prop1: 'value1', prop2: '' } }),
                Step2: createStepConfig('Step2', [
                    { stepName: 'Step1', propertyName: 'prop1', dependentValues: ['value1'] },
                    { stepName: 'Step1', propertyName: 'prop2' }
                ])
            };

            expect(getVisibleStepNames(stepNames, stepConfigurations, {})).toEqual(['Step1']);
        });

        it('should preserve original step order in result', () => {
            const stepNames = ['Step1', 'Step2', 'Step3', 'Step4'];
            const stepConfigurations = {
                Step1: createStepConfig('Step1', [], { group1: { show: 'yes' } }),
                Step2: createStepConfig('Step2', [
                    { stepName: 'Step1', propertyName: 'show', dependentValues: ['no'] }
                ]),
                Step3: createStepConfig('Step3'),
                Step4: createStepConfig('Step4', [
                    { stepName: 'Step1', propertyName: 'show', dependentValues: ['yes'] }
                ])
            };

            expect(getVisibleStepNames(stepNames, stepConfigurations, {})).toEqual(['Step1', 'Step3', 'Step4']);
        });
    });
});
