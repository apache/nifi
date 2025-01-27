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
import { Attribute } from '../../../state/processor-definition';

@Component({
    selector: 'attributes-definition',
    imports: [MatTableModule],
    templateUrl: './attributes-definition.component.html',
    styleUrl: './attributes-definition.component.scss'
})
export class AttributesDefinitionComponent {
    @Input() set attributes(attributes: Attribute[]) {
        this.dataSource.data = this.sortAttributes(attributes);
    }

    displayedColumns: string[] = ['name', 'description'];
    dataSource: MatTableDataSource<Attribute> = new MatTableDataSource<Attribute>();

    constructor(private nifiCommon: NiFiCommon) {}

    sortAttributes(items: Attribute[]): Attribute[] {
        const data: Attribute[] = items.slice();
        return data.sort((a, b) => {
            return this.nifiCommon.compareString(a.name, b.name);
        });
    }
}
