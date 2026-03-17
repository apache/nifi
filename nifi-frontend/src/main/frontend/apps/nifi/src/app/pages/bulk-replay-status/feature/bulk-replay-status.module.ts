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

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { BulkReplayStatus } from './bulk-replay-status.component';
import { BulkReplayStatusRoutingModule } from './bulk-replay-status-routing.module';
import { BulkReplayJobTable } from '../ui/bulk-replay-job-table/bulk-replay-job-table.component';
import { ClearJobsDialog } from '../ui/clear-jobs-dialog/clear-jobs-dialog.component';
import { Navigation } from '../../../ui/common/navigation/navigation.component';

@NgModule({
    declarations: [BulkReplayStatus],
    imports: [
        CommonModule,
        MatDialogModule,
        MatButtonModule,
        BulkReplayStatusRoutingModule,
        Navigation,
        BulkReplayJobTable,
        ClearJobsDialog
    ]
})
export class BulkReplayStatusModule {}
