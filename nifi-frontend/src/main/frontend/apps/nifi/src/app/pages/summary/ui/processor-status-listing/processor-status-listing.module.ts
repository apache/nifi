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

import { NgModule } from '@angular/core';
import { ProcessorStatusListing } from './processor-status-listing.component';
import { CommonModule } from '@angular/common';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { MatSortModule } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogModule } from '@angular/material/dialog';
import { RouterLink } from '@angular/router';
import { NifiTooltipDirective } from '@nifi/shared';
import { SummaryTableFilterModule } from '../common/summary-table-filter/summary-table-filter.module';
import { ProcessorStatusTable } from './processor-status-table/processor-status-table.component';
import { MatPaginatorModule } from '@angular/material/paginator';
import { ProcessGroupStatusTable } from '../process-group-status-listing/process-group-status-table/process-group-status-table.component';
import { RemoteProcessGroupStatusTable } from '../remote-process-group-status-listing/remote-process-group-status-table/remote-process-group-status-table.component';

@NgModule({
    declarations: [ProcessorStatusListing],
    exports: [ProcessorStatusListing],
    imports: [
        CommonModule,
        NgxSkeletonLoaderModule,
        MatFormFieldModule,
        MatInputModule,
        MatOptionModule,
        MatSelectModule,
        MatSortModule,
        MatTableModule,
        ReactiveFormsModule,
        MatDialogModule,
        RouterLink,
        NifiTooltipDirective,
        SummaryTableFilterModule,
        ProcessorStatusTable,
        MatPaginatorModule,
        ProcessGroupStatusTable,
        RemoteProcessGroupStatusTable
    ]
})
export class ProcessorStatusListingModule {}
