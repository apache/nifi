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
import { SystemDiagnosticsRequest, SystemDiagnosticsResponse } from './index';

const SYSTEM_DIAGNOSTICS_PREFIX = '[System Diagnostics]';

export const reloadSystemDiagnostics = createAction(
    `${SYSTEM_DIAGNOSTICS_PREFIX} Load System Diagnostics`,
    props<{ request: SystemDiagnosticsRequest }>()
);

export const loadSystemDiagnosticsSuccess = createAction(
    `${SYSTEM_DIAGNOSTICS_PREFIX} Load System Diagnostics Success`,
    props<{ response: SystemDiagnosticsResponse }>()
);

export const reloadSystemDiagnosticsSuccess = createAction(
    `${SYSTEM_DIAGNOSTICS_PREFIX} Reload System Diagnostics Success`,
    props<{ response: SystemDiagnosticsResponse }>()
);

export const getSystemDiagnosticsAndOpenDialog = createAction(
    `${SYSTEM_DIAGNOSTICS_PREFIX} Get System Diagnostics and Open Dialog`,
    props<{ request: SystemDiagnosticsRequest }>()
);

export const openSystemDiagnosticsDialog = createAction(`${SYSTEM_DIAGNOSTICS_PREFIX} Open System Diagnostics Dialog`);

export const systemDiagnosticsSnackbarError = createAction(
    `${SYSTEM_DIAGNOSTICS_PREFIX} Load System Diagnostics Snackbar Error`,
    props<{ error: string }>()
);

export const systemDiagnosticsBannerError = createAction(
    `${SYSTEM_DIAGNOSTICS_PREFIX} Load System Diagnostics Banner Error`,
    props<{ error: string }>()
);

export const resetSystemDiagnostics = createAction(`${SYSTEM_DIAGNOSTICS_PREFIX} Clear System Diagnostics`);

export const viewSystemDiagnosticsComplete = createAction(
    `${SYSTEM_DIAGNOSTICS_PREFIX} View System Diagnostics Complete`
);
