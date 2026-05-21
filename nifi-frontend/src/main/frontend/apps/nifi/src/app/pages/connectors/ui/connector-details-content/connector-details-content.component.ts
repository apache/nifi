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

import { Component, computed, input } from '@angular/core';

import {
    PropertyGroupCard,
    ConnectorEntity,
    ConfigurationStepConfiguration,
    filterPropertyVisibility,
    ConnectorDetailHeader,
    getVisibleStepNames
} from '@nifi/shared';

@Component({
    selector: 'connector-details-content',
    imports: [PropertyGroupCard, ConnectorDetailHeader],
    templateUrl: './connector-details-content.component.html',
    styleUrls: ['./connector-details-content.component.scss']
})
export class ConnectorDetailsContent {
    connector = input.required<ConnectorEntity>();

    /**
     * Computed signal that returns configuration steps filtered by step visibility,
     * with property groups filtered to only visible properties.
     * Step visibility is based on step dependencies (a step can depend on properties from previous steps).
     * Property visibility is scoped to each step (dependencies can only reference properties within the same step).
     */
    filteredStepConfigurations = computed(() => {
        const steps = this.connector().component.activeConfiguration?.configurationStepConfigurations;
        if (!steps || steps.length === 0) {
            return [];
        }

        const stepConfigurations: { [stepName: string]: ConfigurationStepConfiguration } = {};
        for (const step of steps) {
            stepConfigurations[step.configurationStepName] = step;
        }

        const stepNames = steps.map((s) => s.configurationStepName);
        const visibleStepNames = getVisibleStepNames(stepNames, stepConfigurations, {});

        const visibleSteps = steps.filter((step) => visibleStepNames.includes(step.configurationStepName));
        return filterPropertyVisibility(visibleSteps);
    });
}
