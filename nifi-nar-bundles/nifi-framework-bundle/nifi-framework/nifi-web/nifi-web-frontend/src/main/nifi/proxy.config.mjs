export default {
    '/nifi-api/*': {
        target: 'https://localhost:8443',
        secure: false,
        logLevel: 'debug',
        changeOrigin: true,
        headers: {
            'X-ProxyPort': 4200
        }
    },
    '/nifi-docs/*': {
        target: 'https://localhost:8443',
        secure: false,
        logLevel: 'debug',
        changeOrigin: true,
        headers: {
            'X-ProxyPort': 4200
        }
    },
    '/nifi-content-viewer/*': {
        target: 'https://localhost:8443',
        secure: false,
        logLevel: 'debug',
        changeOrigin: true,
        headers: {
            'X-ProxyPort': 4200
        }
    },
    // the following entry is needed because the content viewer (and other UIs) load resources from existing nifi ui
    '/nifi/*': {
        target: 'https://localhost:8443',
        secure: false,
        logLevel: 'debug',
        changeOrigin: true,
        headers: {
            'X-ProxyPort': 4200
        }
    }
};
