// AboutService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export interface AboutResponse {
  // Define properties based on expected API response
  about?: any;
}

export function getAbout$(): Observable<AboutResponse> {
  return ajax.getJSON<AboutResponse>(`${API}/flow/about`).pipe(
    map(response => response)
  );
}
