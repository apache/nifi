#!groovy
@Library(value='pipeline-lib@master', changelog=false) _

buildPipeline projectName: 'alp-nifi-base',
              dockerFilesInfo: [
                ["fileNameWithPath":"./nifi-docker/dockermaven/Dockerfile", "baseContextPath": ".", "imageName": "alp-nifi-base"],
                ["fileNameWithPath":"./nifi-registry/nifi-registry-docker-maven/dockermaven/Dockerfile", "baseContextPath": ".", "imageName": "alp-nifi-registry-base"],
                
              ],
              runAnchore: [defaultBranches: true, featureBranches: false],
              mapForBranchBasedTag: [:]