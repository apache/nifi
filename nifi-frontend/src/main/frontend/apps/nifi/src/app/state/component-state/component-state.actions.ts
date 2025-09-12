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

import { createAction, props } from '@ngrx/store';
import { ComponentStateRequest, ComponentStateResponse, ClearStateEntryRequest } from './index';
import { ErrorContext } from '../error';

const COMPONENT_STATE_PREFIX = '[Component State]';

export const getComponentStateAndOpenDialog = createAction(
    `${COMPONENT_STATE_PREFIX} Get Component State and Open Dialog`,
    props<{ request: ComponentStateRequest }>()
);

export const loadComponentStateSuccess = createAction(
    `${COMPONENT_STATE_PREFIX} Load Component State Success`,
    props<{ response: ComponentStateResponse }>()
);

export const openComponentStateDialog = createAction(`${COMPONENT_STATE_PREFIX} Open Component State Dialog`);

export const clearComponentState = createAction(`${COMPONENT_STATE_PREFIX} Clear Component State`);

export const clearComponentStateEntry = createAction(
    `${COMPONENT_STATE_PREFIX} Clear Component State Entry`,
    props<{ request: ClearStateEntryRequest }>()
);

export const clearComponentStateFailure = createAction(
    `${COMPONENT_STATE_PREFIX} Clear Component State Failure`,
    props<{ errorContext: ErrorContext }>()
);

export const reloadComponentState = createAction(`${COMPONENT_STATE_PREFIX} Reload Component State`);

export const reloadComponentStateSuccess = createAction(
    `${COMPONENT_STATE_PREFIX} Reload Component State Success`,
    props<{ response: ComponentStateResponse }>()
);

export const resetComponentState = createAction(`${COMPONENT_STATE_PREFIX} Reset Component State`);
