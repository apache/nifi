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
import { BulletinBoardFilterArgs, LoadBulletinBoardRequest, LoadBulletinBoardResponse } from './index';

const BULLETIN_BOARD_PREFIX = '[Bulletin Board]';

export const loadBulletinBoard = createAction(
    `${BULLETIN_BOARD_PREFIX} Load Bulletin Board`,
    props<{ request: LoadBulletinBoardRequest }>()
);

export const loadBulletinBoardSuccess = createAction(
    `${BULLETIN_BOARD_PREFIX} Load Bulletin Board Success`,
    props<{ response: LoadBulletinBoardResponse }>()
);

export const resetBulletinBoardState = createAction(`${BULLETIN_BOARD_PREFIX} Reset Bulletin Board State`);

export const clearBulletinBoard = createAction(`${BULLETIN_BOARD_PREFIX} Clear Bulletin Board`);

export const setBulletinBoardFilter = createAction(
    `${BULLETIN_BOARD_PREFIX} Set Bulletin Board Filter`,
    props<{ filter: BulletinBoardFilterArgs }>()
);

export const setBulletinBoardAutoRefresh = createAction(
    `${BULLETIN_BOARD_PREFIX} Set Auto-Refresh`,
    props<{ autoRefresh: boolean }>()
);

export const startBulletinBoardPolling = createAction(`${BULLETIN_BOARD_PREFIX} Start polling`);

export const stopBulletinBoardPolling = createAction(`${BULLETIN_BOARD_PREFIX} Stop polling`);
