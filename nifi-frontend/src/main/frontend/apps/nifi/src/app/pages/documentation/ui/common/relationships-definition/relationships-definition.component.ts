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

import { Component, Input } from '@angular/core';
import { NiFiCommon } from '@nifi/shared';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { Relationship } from '../../../state/processor-definition';

@Component({
    selector: 'relationships-definition',
    imports: [MatTableModule],
    templateUrl: './relationships-definition.component.html',
    styleUrl: './relationships-definition.component.scss'
})
export class RelationshipsDefinitionComponent {
    @Input() set relationships(relationships: Relationship[]) {
        this.dataSource.data = this.sortRelationships(relationships);
    }

    displayedColumns: string[] = ['name', 'description'];
    dataSource: MatTableDataSource<Relationship> = new MatTableDataSource<Relationship>();

    constructor(private nifiCommon: NiFiCommon) {}

    sortRelationships(items: Relationship[]): Relationship[] {
        const data: Relationship[] = items.slice();
        return data.sort((a, b) => {
            return this.nifiCommon.compareString(a.name, b.name);
        });
    }
}
