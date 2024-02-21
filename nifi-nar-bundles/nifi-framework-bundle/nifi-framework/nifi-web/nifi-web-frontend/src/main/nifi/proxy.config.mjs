const target = {
    target: 'https://localhost:8443',
    secure: false,
    logLevel: 'debug',
    changeOrigin: true,
    headers: {
        'X-ProxyPort': 4200
    }
};

export default {
    '/': target
};
