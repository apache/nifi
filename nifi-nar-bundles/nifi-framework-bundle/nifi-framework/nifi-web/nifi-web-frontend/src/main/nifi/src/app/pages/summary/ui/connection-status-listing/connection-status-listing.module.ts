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
import { ConnectionStatusListing } from './connection-status-listing.component';
import { CommonModule } from '@angular/common';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { PortStatusTable } from '../common/port-status-table/port-status-table.component';
import { ConnectionStatusTable } from './connection-status-table/connection-status-table.component';
import { ProcessorStatusTable } from '../processor-status-listing/processor-status-table/processor-status-table.component';

@NgModule({
    declarations: [ConnectionStatusListing],
    exports: [ConnectionStatusListing],
    imports: [CommonModule, NgxSkeletonLoaderModule, PortStatusTable, ConnectionStatusTable, ProcessorStatusTable]
})
export class ConnectionStatusListingModule {}
