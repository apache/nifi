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
import { LoadUserResponse, UserState } from './index';
import { LoadProcessGroupRequest, LoadProcessGroupResponse } from '../../pages/flow-designer/state/flow';

export const loadUser = createAction('[User] Load User');

export const loadUserSuccess = createAction('[User] Load User Success', props<{ response: LoadUserResponse }>());

export const userApiError = createAction('[User] User Api Error', props<{ error: string }>());

export const clearUserApiError = createAction('[User] Clear User Api Error');

export const startUserPolling = createAction('[User] Start User Polling');

export const stopUserPolling = createAction('[User] Stop User Polling');
