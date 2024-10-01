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
import { ReportingTaskDefinition } from './index';
import { DefinitionCoordinates } from '../index';

const REPORTING_TASK_DEFINITION_PREFIX = '[Reporting Task Definition]';

export const loadReportingTaskDefinition = createAction(
    `${REPORTING_TASK_DEFINITION_PREFIX} Load Reporting Task Definition`,
    props<{ coordinates: DefinitionCoordinates }>()
);

export const loadReportingTaskDefinitionSuccess = createAction(
    `${REPORTING_TASK_DEFINITION_PREFIX} Load Reporting Task Definition Success`,
    props<{ reportingTaskDefinition: ReportingTaskDefinition }>()
);

export const reportingTaskDefinitionApiError = createAction(
    `${REPORTING_TASK_DEFINITION_PREFIX} Load Reporting Task Definition Error`,
    props<{ error: string }>()
);

export const resetReportingTaskDefinitionState = createAction(
    `${REPORTING_TASK_DEFINITION_PREFIX} Reset Reporting Task Definition State`
);
