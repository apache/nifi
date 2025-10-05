// CurrentUserService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export interface CurrentUserResponse {
  // Define properties based on expected API response
  user?: any;
}

export function getUser$(): Observable<CurrentUserResponse> {
  return ajax.getJSON<CurrentUserResponse>(`${API}/flow/current-user`).pipe(
    map(response => response)
  );
}
