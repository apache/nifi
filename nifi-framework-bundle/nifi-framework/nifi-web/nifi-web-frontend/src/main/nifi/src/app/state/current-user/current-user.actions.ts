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
import { LoadCurrentUserResponse } from './index';

export const loadCurrentUser = createAction('[Current User] Load Current User');

export const loadCurrentUserSuccess = createAction(
    '[Current User] Load Current User Success',
    props<{ response: LoadCurrentUserResponse }>()
);

export const startCurrentUserPolling = createAction('[Current User] Start Current User Polling');

export const stopCurrentUserPolling = createAction('[Current User] Stop Current User Polling');
