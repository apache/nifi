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

import { Component, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { ExtensionCreation } from '../../../../../ui/common/extension-creation/extension-creation.component';
import { ReportingTasksState } from '../../../state/reporting-tasks';
import { createReportingTask } from '../../../state/reporting-tasks/reporting-tasks.actions';
import { Client } from '../../../../../service/client.service';
import { DocumentedType } from '../../../../../state/shared';
import { selectSaving } from '../../../state/reporting-tasks/reporting-tasks.selectors';
import { AsyncPipe } from '@angular/common';
import { CloseOnEscapeDialog } from '@nifi/shared';
import {
    selectExtensionTypesLoadingStatus,
    selectReportingTaskTypes
} from '../../../../../state/extension-types/extension-types.selectors';

@Component({
    selector: 'create-reporting-task',
    imports: [ExtensionCreation, AsyncPipe],
    templateUrl: './create-reporting-task.component.html',
    styleUrls: ['./create-reporting-task.component.scss']
})
export class CreateReportingTask extends CloseOnEscapeDialog {
    private store = inject<Store<ReportingTasksState>>(Store);
    private client = inject(Client);

    reportingTaskTypes$ = this.store.select(selectReportingTaskTypes);
    reportingTaskTypesLoadingStatus$ = this.store.select(selectExtensionTypesLoadingStatus);
    saving$ = this.store.select(selectSaving);

    createReportingTask(reportingTaskType: DocumentedType): void {
        this.store.dispatch(
            createReportingTask({
                request: {
                    revision: {
                        clientId: this.client.getClientId(),
                        version: 0
                    },
                    reportingTaskType: reportingTaskType.type,
                    reportingTaskBundle: reportingTaskType.bundle
                }
            })
        );
    }
}
