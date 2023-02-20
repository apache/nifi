#!groovy
@Library(value='pipeline-lib@master', changelog=false) _

buildPipeline projectName: 'alp-nifi-base',
              dockerFilesInfo: [
                ["fileNameWithPath":"./nifi-docker/dockerhub/Dockerfile", "baseContextPath": ".", "imageName": "alp-nifi-base"],
              ],
              runAnchore: [defaultBranches: true, featureBranches: false],
              mapForBranchBasedTag: ['main': 'main']