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

import { Injectable } from '@angular/core';
import { Observable, of, switchMap, takeUntil } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { NewMapTableEntryDialog } from '../components/new-map-table-entry-dialog/new-map-table-entry-dialog.component';
import { MapTableEntry, MapTableEntryData, SMALL_DIALOG } from '../types';

@Injectable({
    providedIn: 'root'
})
export class MapTableHelperService {
    constructor(private dialog: MatDialog) {}

    /**
     * Returns a function that can be used to pass into a MapTable to support creating a new entry.
     *
     * @param entryType      string representing the type of Map Table Entry
     */
    createNewEntry(entryType: string): (existingEntries: string[]) => Observable<MapTableEntry> {
        return (existingEntries: string[]) => {
            const dialogRef = this.dialog.open(NewMapTableEntryDialog, {
                ...SMALL_DIALOG,
                data: {
                    existingEntries,
                    entryTypeLabel: entryType
                } as MapTableEntryData
            });
            return dialogRef.componentInstance.newEntry.pipe(
                takeUntil(dialogRef.afterClosed()),
                switchMap((name: string) => {
                    dialogRef.close();
                    const newEntry: MapTableEntry = {
                        name,
                        value: null
                    };
                    return of(newEntry);
                })
            );
        };
    }
}
