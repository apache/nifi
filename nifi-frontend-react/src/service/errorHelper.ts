// ErrorHelper migrated from Angular with TypeScript support

export interface ErrorDetail {
  title: string;
  message: string;
}

export interface FullScreenErrorAction {
  type: 'fullScreenError';
  payload: {
    skipReplaceUrl?: boolean;
    errorDetail: ErrorDetail;
  };
}

export interface SnackBarErrorAction {
  type: 'snackBarError';
  payload: {
    error: string;
  };
}

export type ErrorAction = FullScreenErrorAction | SnackBarErrorAction;

export interface ErrorResponse {
  status: number;
  message?: string;
  error?: unknown;
}

const FULL_SCREEN_TITLES: Record<number, string> = {
  401: 'Unauthorized',
  403: 'Insufficient Permissions',
  409: 'Invalid State',
  413: 'Payload Too Large'
};

export function fullScreenError(
  errorResponse: ErrorResponse,
  skipReplaceUrl = false
): FullScreenErrorAction {
  const title = FULL_SCREEN_TITLES[errorResponse.status] ?? 'An unexpected error has occurred';

  const message =
    errorResponse.status === 0 || errorResponse.error == null
      ? 'An error occurred communicating with NiFi. Please check the logs and fix any configuration issues before restarting.'
      : errorResponse.status === 401
      ? 'Your session has expired. Please navigate home to log in again.'
      : getErrorString(errorResponse);

  return {
    type: 'fullScreenError',
    payload: {
      skipReplaceUrl,
      errorDetail: {
        title,
        message
      }
    }
  };
}

export function showErrorInContext(status: number): boolean {
  return [400, 403, 404, 409, 413, 503].includes(status);
}

export function handleLoadingError(
  status: string,
  errorResponse: ErrorResponse
): ErrorAction {
  if (status === 'success') {
    if (showErrorInContext(errorResponse.status)) {
      return {
        type: 'snackBarError',
        payload: {
          error: getErrorString(errorResponse)
        }
      };
    }
    return fullScreenError(errorResponse);
  }

  return fullScreenError(errorResponse);
}

export function getErrorString(errorResponse: ErrorResponse, prefix?: string): string {
  let errorMessage = 'An unspecified error occurred.';

  if (errorResponse.status !== 0) {
    if (typeof errorResponse.error === 'string') {
      errorMessage = errorResponse.error;
    } else if (errorResponse.error && typeof errorResponse.error === 'object') {
      errorMessage = JSON.stringify(errorResponse.error);
    } else if (errorResponse.message) {
      errorMessage = errorResponse.message;
    } else {
      errorMessage = `${errorResponse.status}`;
    }
  }

  return prefix ? `${prefix} - [${errorMessage}]` : errorMessage;
}
