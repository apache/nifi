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
import { GarbageCollectionTipInput } from '../../../../state/shared';
import { GarbageCollection } from '../../../../state/system-diagnostics';

@Component({
    selector: 'garbage-collection-tip',
    imports: [],
    templateUrl: './garbage-collection-tip.component.html',
    styleUrl: './garbage-collection-tip.component.scss'
})
export class GarbageCollectionTip {
    garbageCollections: GarbageCollection[] = [];

    @Input() set data(data: GarbageCollectionTipInput | undefined) {
        if (data?.garbageCollections) {
            this.garbageCollections = [...data.garbageCollections];
            this.garbageCollections.sort((a, b) => {
                return b.collectionCount - a.collectionCount;
            });
        } else {
            this.garbageCollections = [];
        }
    }
}
