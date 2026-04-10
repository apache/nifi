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

import { Injectable } from '@angular/core';
import { ConfigurationStepConfiguration } from '../types';

/**
 * Interface for step components that support saving their configuration.
 * Decouples the wizard parent from concrete step component classes.
 *
 * `stepName` is a callable that returns the step's name. This accommodates
 * both Angular signal inputs (`input.required<string>()`) and plain
 * arrow-function properties (`stepName = () => 'Name'`).
 */
export interface SaveableStep {
    stepName: () => string;
    getConfigurationForSave(): {
        stepName: string;
        configuration: ConfigurationStepConfiguration | null;
        isDirty: boolean;
    };
}

/**
 * Service for tracking the currently active wizard step.
 * Provided at the wizard-parent level so that step components can register
 * themselves and the wizard parent can read back the active step without
 * needing a direct component reference or template binding.
 *
 * Usage:
 *   1. The wizard parent provides this service via its `providers` array.
 *   2. Each step component injects this service (optionally) and calls
 *      `register(this)` during initialization.
 *   3. The wizard parent injects this service and reads `activeStep`
 *      when it needs to access the current step's configuration.
 */
@Injectable()
export class ActiveStepService {
    private _activeStep: SaveableStep | null = null;

    /**
     * The currently registered active step, or null if no step is active.
     */
    get activeStep(): SaveableStep | null {
        return this._activeStep;
    }

    /**
     * Register a step as the currently active step.
     * Called by step components during initialization.
     */
    register(step: SaveableStep): void {
        this._activeStep = step;
    }

    /**
     * Unregister the given step if it is the currently active step.
     * Called by step components during destruction to prevent stale references.
     */
    unregister(step: SaveableStep): void {
        if (this._activeStep === step) {
            this._activeStep = null;
        }
    }
}
