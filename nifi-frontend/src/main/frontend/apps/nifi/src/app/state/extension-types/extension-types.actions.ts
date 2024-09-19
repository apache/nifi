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

import { createAction, props } from '@ngrx/store';
import {
    LoadExtensionTypesForCanvasResponse,
    LoadExtensionTypesForDocumentationResponse,
    LoadExtensionTypesForPoliciesResponse,
    LoadExtensionTypesForSettingsResponse
} from './index';
import { HttpErrorResponse } from '@angular/common/http';

export const loadExtensionTypesForCanvas = createAction('[Extension Types] Load Extension Types For Canvas');

export const loadExtensionTypesForCanvasSuccess = createAction(
    '[Extension Types] Load Extension Types For Canvas Success',
    props<{ response: LoadExtensionTypesForCanvasResponse }>()
);

export const loadExtensionTypesForSettings = createAction('[Extension Types] Load Extension Types For Settings');

export const loadExtensionTypesForSettingsSuccess = createAction(
    '[Extension Types] Load Extension Types For Settings Success',
    props<{ response: LoadExtensionTypesForSettingsResponse }>()
);

export const loadExtensionTypesForPolicies = createAction('[Extension Types] Load Extension Types For Policies');

export const loadExtensionTypesForPoliciesSuccess = createAction(
    '[Extension Types] Load Extension Types For Policies Success',
    props<{ response: LoadExtensionTypesForPoliciesResponse }>()
);

export const loadExtensionTypesForDocumentation = createAction(
    '[Extension Types] Load Extension Types For Documentation'
);

export const loadExtensionTypesForDocumentationSuccess = createAction(
    '[Extension Types] Load Extension Types For Documentation Success',
    props<{ response: LoadExtensionTypesForDocumentationResponse }>()
);

export const extensionTypesApiError = createAction(
    '[Extension Types] Extension Types Api Error',
    props<{ error: HttpErrorResponse }>()
);
