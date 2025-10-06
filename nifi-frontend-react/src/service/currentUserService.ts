// CurrentUserService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

import type { CurrentUser } from '../state/currentUserSlice';

export const API = '../nifi-api';

export function getUser$(): Observable<CurrentUser> {
  return ajax
    .getJSON<CurrentUser>(`${API}/flow/current-user`)
    .pipe(map((response) => response));
}
