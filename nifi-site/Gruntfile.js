/* global module */

module.exports = function (grunt) {
    // Project configuration.
    grunt.initConfig({
        config: {
            svn: {
                url: 'https://svn.apache.org/repos/asf/incubator/nifi/site/trunk'
            }
        },
        pkg: grunt.file.readJSON('package.json'),
        clean: {
            options: {
                force: true
            },
            js: ['dist/js/'],
            css: ['dist/css/'],
            assets: ['dist/assets/*'],
            generated: ['dist/docs'],
            all: ['dist']
        },
        assemble: {
            options: {
                partials: 'src/includes/*.hbs',
                layout: 'src/layouts/html.hbs',
                flatten: true
            },
            html: {
                files: {
                    'dist/': ['src/pages/html/*.hbs']
                }
            },
            markdown: {
                options: {
                    layout: 'src/layouts/markdown.hbs'
                },
                files: {
                    'dist/': ['src/pages/markdown/*.md']
                }
            }
        },
        compass: {
            dist: {
                options: {
                    config: 'config.rb'
                }
            }
        },
        concat: {
            options: {
                separator: ';'
            },
            foundation: {
                src: [
                    'bower_components/foundation/js/foundation/foundation.js',
                    'bower_components/foundation/js/foundation/foundation.topbar.js',
                    'bower_components/foundation/js/foundation/foundation.reveal.js'
                ],
                dest: 'dist/assets/js/foundation.js'
            },
            modernizr: {
                src: [
                    'bower_components/modernizr/modernizr.js'
                ],
                dest: 'dist/assets/js/modernizr.js'
            },
            nifi: {
                src: [
                    'src/js/app.js'
                ],
                dest: 'dist/js/app.js'
            }
        },
        copy: {
            generated: {
                files: [{
                        expand: true,
                        cwd: '../nifi/nifi-docs/target/generated-docs',
                        src: ['*.html', 'images/*'],
                        dest: 'dist/docs/'
                    }, {
                        expand: true,
                        cwd: '../nifi/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api',
                        src: ['target/nifi-web-api-*/docs/rest-api/index.html', 'target/nifi-web-api-*/docs/rest-api/images/*'],
                        dest: 'dist/docs/',
                        rename: function (dest, src) {
                            var path = require('path');

                            if (src.indexOf('images') > 0) {
                                return path.join(dest, 'rest-api/images', path.basename(src));
                            } else {
                                return path.join(dest, 'rest-api', path.basename(src));
                            }
                        }
                    }]
            },
            dist: {
                files: [{
                        expand: true,
                        cwd: 'src/images/',
                        src: ['**/*.{png,jpg,gif,svg,ico}'],
                        dest: 'dist/images/'
                    }, {
                        expand: true,
                        cwd: 'bower_components/jquery/dist',
                        src: ['jquery.min.js'],
                        dest: 'dist/assets/js/'
                    }, {
                        expand: true,
                        cwd: 'bower_components/webfontloader',
                        src: ['webfontloader.js'],
                        dest: 'dist/assets/js/'
                    }, {
                        expand: true,
                        cwd: 'bower_components/font-awesome/css',
                        src: ['font-awesome.min.css'],
                        dest: 'dist/assets/stylesheets/'
                    }, {
                        expand: true,
                        cwd: 'bower_components/font-awesome',
                        src: ['fonts/*'],
                        dest: 'dist/assets/'
                    }]
            }
        },
        prompt: {
            username: {
                options: {
                    questions: [{
                            config: 'config.svn.username',
                            type: 'input',
                            message: 'Enter SVN username (if different from current or configured user):'
                    }]
                }
            },
            commit: {
                options: {
                    questions: [{
                            config: 'config.svn.commit.confirmation',
                            type: 'list',
                            choices: ['Show diff', 'Commit', 'Abort'],
                            message: 'Commit the changes listed above?'
                    }],
                    then: function (results) {
                        if (results['config.svn.commit.confirmation'] === 'Commit') {
                            grunt.task.run('prompt:message');
                        } else if (results['config.svn.commit.confirmation'] === 'Show diff') {
                            grunt.task.run('exec:diff');
                            grunt.task.run('prompt:commit');
                        }
                    }
                }
            },
            message: {
                options: {
                    questions: [{
                            config: 'config.svn.commit.message',
                            type: 'input',
                            message: 'Commit message:'
                    }, {
                            config: 'config.svn.password',
                            type: 'password',
                            message: 'SVN password (if different from configured):'
                    }],
                    then: function () {
                        grunt.task.run('exec:add');
                        grunt.task.run('exec:commit');
                    }
                }
            }
        },
        exec: {
            generateDocs: {
                command: 'mvn clean package',
                cwd: '../nifi/nifi-docs',
                stdout: true,
                stderr: true
            },
            generateRestApiDocs: {
                command: 'mvn clean package -DskipTests',
                cwd: '../nifi/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api',
                stdout: true,
                stderr: true
            },
            checkout: {
                command: function() {
                    var url = grunt.config('config.svn.url');
                    var username = grunt.config('config.svn.username');
                    var command = 'svn checkout';
                    if (username !== '') {
                        command += (' --username ' + username);
                    }
                    return command + ' ' + url + ' --trust-server-cert --non-interactive dist';
                },
                stdout: true,
                stderr: true
            },
            status: {
                cwd: 'dist',
                command: 'svn status',
                stdout: true,
                stderr: true
            },
            diff: {
                cwd: 'dist',
                command: 'svn diff',
                stdout: true,
                stderr: true
            },
            add: {
                cwd: 'dist',
                command: 'svn add --force .',
                stdout: true,
                stderr: true
            },
            commit: {
                cwd: 'dist',
                command: function() {
                    var username = grunt.config('config.svn.username');
                    var password = grunt.config('config.svn.password');
                    var message = grunt.config('config.svn.commit.message');
                    var command = 'svn commit';
                    if (username !== '') {
                        command += (' --username ' + username);
                    }
                    if (password !== '') {
                        command += (' --password ' + password);
                    }
                    return command + ' -m "' + message + '" --trust-server-cert --non-interactive .';
                }
            }
        },
        replace: {
            addGoogleAnalytics: {
                src: ['dist/docs/*.html', 'dist/docs/rest-api/index.html'],
                overwrite: true,
                replacements: [{
                        from: /<\/head>/g,
                        to: "<script>\n" +
                                    "(function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){\n" +
                                    "(i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),\n" +
                                    "m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)\n" +
                                    "})(window,document,'script','//www.google-analytics.com/analytics.js','ga');\n" +
                                    "ga('create', 'UA-57264262-1', 'auto');\n" +
                                    "ga('send', 'pageview');\n" +
                                "</script>\n" +
                            "</head>"
                    }]
            },
            moveTearDrop: {
                src: ['dist/docs/rest-api/index.html'],
                overwrite: true,
                replacements: [{
                        from: /<img class="logo" src="images\/bgNifiLogo.png" alt="NiFi Logo"\/>/g,
                        to: '<img class="logo" src="images/bgNifiLogo.png" alt="NiFi Logo" style="float: right;"/>'
                }]
            },
            removeVersion: {
                src: ['dist/docs/rest-api/index.html'],
                overwrite: true,
                replacements: [{
                        from: /<div class="sub-title">.*<\/div>/g,
                        to: '<div class="sub-title">NiFi Rest Api</div>'
                }, {
                        from: /<title>.*<\/title>/g,
                        to: '<title>NiFi Rest Api</title>'
                }]
            }
        },
        watch: {
            grunt: {
                files: ['Gruntfile.js'],
                tasks: ['dev']
            },
            css: {
                files: 'src/scss/*.scss',
                tasks: ['css']
            },
            script: {
                files: 'src/js/*.js',
                tasks: ['js']
            },
            images: {
                files: 'src/images/*.{png,jpg,gif,svg,ico}',
                tasks: ['img']
            },
            assemble: {
                files: ['src/{includes,layouts}/*.hbs', 'src/pages/{html,markdown}/*.{hbs,md}'],
                tasks: ['assemble']
            }
        }
    });

    grunt.loadNpmTasks('grunt-newer');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('assemble');
    grunt.loadNpmTasks('grunt-contrib-compass');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-exec');
    grunt.loadNpmTasks('grunt-text-replace');
    grunt.loadNpmTasks('grunt-prompt');

    grunt.registerTask('img', ['newer:copy']);
    grunt.registerTask('css', ['clean:css', 'compass']);
    grunt.registerTask('js', ['clean:js', 'concat']);
    grunt.registerTask('generate-docs', ['clean:generated', 'exec:generateDocs', 'exec:generateRestApiDocs', 'copy:generated', 'replace:addGoogleAnalytics', 'replace:moveTearDrop', 'replace:removeVersion']);

    grunt.registerTask('build', ['assemble', 'css', 'js', 'img', 'generate-docs', 'copy:dist']);
    grunt.registerTask('deploy', ['clean:all', 'prompt:username', 'exec:checkout', 'build', 'exec:status', 'prompt:commit']);
    grunt.registerTask('dev', ['default', 'watch']);
    
    grunt.registerTask('default', ['clean:all', 'build']);
};
