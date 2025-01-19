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

import { Bundle, ComponentHistory, DocumentedType } from '../../../../state/shared';
import { BulletinEntity, Permissions, Revision } from '@nifi/shared';

export const reportingTasksFeatureKey = 'reportingTasks';

export interface CreateReportingTaskDialogRequest {
    reportingTaskTypes: DocumentedType[];
}

export interface LoadReportingTasksResponse {
    reportingTasks: ReportingTaskEntity[];
    loadedTimestamp: string;
}

export interface CreateReportingTaskRequest {
    reportingTaskType: string;
    reportingTaskBundle: Bundle;
    revision: Revision;
}

export interface CreateReportingTaskSuccess {
    reportingTask: ReportingTaskEntity;
}

export interface ConfigureReportingTaskRequest {
    id: string;
    uri: string;
    payload: any;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface ConfigureReportingTaskSuccess {
    id: string;
    reportingTask: ReportingTaskEntity;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface UpdateReportingTaskRequest {
    payload: any;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface EditReportingTaskDialogRequest {
    id: string;
    reportingTask: ReportingTaskEntity;
    history?: ComponentHistory;
}

export interface StartReportingTaskRequest {
    reportingTask: ReportingTaskEntity;
}

export interface StartReportingTaskSuccess {
    reportingTask: ReportingTaskEntity;
}

export interface StopReportingTaskRequest {
    reportingTask: ReportingTaskEntity;
}

export interface StopReportingTaskSuccess {
    reportingTask: ReportingTaskEntity;
}

export interface DeleteReportingTaskRequest {
    reportingTask: ReportingTaskEntity;
}

export interface DeleteReportingTaskSuccess {
    reportingTask: ReportingTaskEntity;
}

export interface SelectReportingTaskRequest {
    id: string;
}

export interface ReportingTaskEntity {
    permissions: Permissions;
    operatePermissions?: Permissions;
    revision: Revision;
    bulletins: BulletinEntity[];
    id: string;
    uri: string;
    status: any;
    component: any;
}

export interface ReportingTasksState {
    reportingTasks: ReportingTaskEntity[];
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}
