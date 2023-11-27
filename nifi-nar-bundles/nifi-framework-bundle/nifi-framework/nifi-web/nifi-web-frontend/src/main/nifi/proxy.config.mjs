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
    }
};
