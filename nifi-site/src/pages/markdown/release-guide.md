---
title: Apache NiFi Release Guidelines
---

# Apache NiFi Release Guidelines

The purpose of this document is to capture and describe the steps involved in producing 
an official release of Apache NiFi.  It is written specifically to someone acting in the
capacity of a [Release Manager][release-manager] (RM).  

## Background Material

  - These documents are necessary for all committers to be familiar with
    - [Apache License V2.0][apache-license]
    - [Apache Legal License/Resolved][apache-legal-resolve]
    - [Apache How-to Apply License][apache-license-apply]

  - These documents are necessary for someone acting as the RM
    - [Apache Encryption Software / ECCN Info][apache-encryption]
    - [Apache Release Policy][apache-release-policy]
    - [Apache Release Guide][apache-release-guide]

  - These documents are helpful for general environmental setup to perform releases
    - [Apache PGP Info][apache-pgp]
    - [Apache Release Signing][apache-release-signing]
    - [Apache Guide to publish Maven Artifacts][apache-guide-publish-maven]

## The objective

Our aim is to produce an official Apache release.  
The following is a list of the sorts of things that will be validated and are the basics to check
when evaluating a release for a vote.

## What to validate and how to Validate a release

  - Are LICENSE and NOTICE file present in the source root and complete?
    - Specifically look in the *-sources.zip artifact and ensure these items are present at the root of the archive.
  - Evaluate the sources and dependencies.  Does the overall LICENSE and NOTICE appear correct?  Do all licenses fit within the ASF approved licenses?
    - Here is an example path to a sources artifact:  
      - `https://repository.apache.org/service/local/repositories/orgapachenifi-1011/content/org/apache/nifi/nifi/0.0.1/nifi-0.0.1-source-release.zip`
  - Is there a README available that explains how to build the application and to execute it?
    - Look in the *-sources.zip artifact root for the readme.
  - Are the signatures and hashes correct for the source release?
    - Validate the hashes of the sources artifact do in fact match:
      - `https://repository.apache.org/service/local/repositories/orgapachenifi-1011/content/org/apache/nifi/nifi/0.0.1/nifi-0.0.1-source-release.zip.md5`
      - `https://repository.apache.org/service/local/repositories/orgapachenifi-1011/content/org/apache/nifi/nifi/0.0.1/nifi-0.0.1-source-release.zip.sha1`
    - Validate the signature of the source artifact.  Here is an example path:
      - `https://repository.apache.org/service/local/repositories/orgapachenifi-1011/content/org/apache/nifi/nifi/0.0.1/nifi-0.0.1-source-release.zip.asc`
      - Need a quick reminder on how to [verify a signature](http://www.apache.org/dev/release-signing.html#verifying-signature)?
  - Do all sources have necessary headers?
    - Unzip the sources file into a directory and execute `mvn install -Pcontrib-check`
  - Are there no unexpected binary files in the release?
    - The only thing we'd expect would be potentially test resources files.
  - Does the app (if appropriate) execute and function as expected?
  
## The flow of a release (an outline)
  - The community is contributing to a series of JIRA tickets assigned to the next release
  - The number of tickets open/remaining for that next release approaches zero
  - A member of the community suggests a release and initiates a discussion
  - Someone volunteers to be an RM for the release (can be a committer but apache guides indicate preference is a PMC member)
  - A release candidate is put together and a vote sent to the team.
  - If the NiFi community rejects the vote the issues noted are resolved and another RC is generated
  - If the NiFi community accepts the vote then the release is 'releasable' and can be placed into the appropriate 'dist' location, maven artifacts released from staging.
  
## The mechanics of the release

### Prepare your environment
  
Follow the steps outlined in the [Quickstart Guide][quickstart-guide]

```        
At this point you're on the latest 'develop' branch and are able to build the entire application
```
<br/>
Create a JIRA ticket for the release tasks and use that ticket number for the commit messages.  For example we'll consider NIFI-270 as our ticket.  Also
have in mind the release version you are planning for.  For example we'll consider '0.0.1'.

Create the next version in JIRA if necessary so develop work can continue towards that release.

Create new branch off develop named after the JIRA ticket or just use the develop branch itself.  Here we'll use a branch off of develop with
`git checkout -b NIFI-270-RC1`

Change directory into that of the project you wish to release.  For example `cd nifi`

Verify that Maven has sufficient heap space to perform the build tasks.  Some plugins and parts of the build 
consumes a surprisingly large amount of space.  These settings have been shown to 
work `MAVEN_OPTS="-Xms1024m -Xmx3076m -XX:MaxPermSize=256m"`

Ensure your settings.xml has been updated as shown below.  There are other ways to ensure your PGP key is available for signing as well
  
```
        ...
        <profile>
           <id>signed_release</id>
           <properties>
               <mavenExecutorId>forked-path</mavenExecutorId>
               <gpg.keyname>YOUR GPG KEY ID HERE</gpg.keyname>
               <gpg.passphrase>YOUR GPG PASSPHRASE HERE</gpg.passphrase>
           </properties>
       </profile>
       ...
       <servers>
          <server>
              <id>repository.apache.org</id>
              <username>YOUR USER NAME HERE</username>
              <password>YOUR MAVEN ENCRYPTED PASSWORD HERE</password>
          </server>
       </servers>
       ...
```

Ensure the the full application build and tests all work by executing
`mvn -T 2.5C clean install` for a parallel build.  Once that completes you can
startup and test the application by `cd nifi-assembly/target` then run `bin/nifi.sh start` in the nifi build.
The application should be up and running in a few seconds at `http://localhost:8080/nifi`

Evaluate and ensure the appropriate license headers are present on all source files.  Ensure LICENSE and NOTICE files are complete and accurate.  
Developers should always be keeping these up to date as they go along adding source and modifying dependencies to keep this burden manageable.  
This command `mvn install -Pcontrib-check` should be run as well to help validate.  If that doesn't complete cleanly it must be addressed.

Now its time to have maven prepare the release so execute `mvn release:prepare -Psigned_release -DscmCommentPrefix="NIFI-270-RC1 " -Darguments="-DskipTests"`.
Maven will ask:

`What is the release version for "Apache NiFi"? (org.apache.nifi:nifi) 0.0.1: :`

Just hit enter to accept the default.

Maven will then ask:

`What is SCM release tag or label for "Apache NiFi"? (org.apache.nifi:nifi) nifi-0.0.1: : `

Enter `nifi-0.0.1-RC1` or whatever the appropriate release candidate (RC) number is.
Maven will then ask:

`What is the new development version for "Apache NiFi"? (org.apache.nifi:nifi) 0.0.2-SNAPSHOT: :`

Just hit enter to accept the default.

Now that preparation went perfectly it is time to perform the release and deploy artifacts to staging.  To do that execute

`mvn release:perform -Psigned_release -DscmCommentPrefix="NIFI-270-RC1 " -Darguments="-DskipTests"`

That will complete successfully and this means the artifacts have been released to the Apache Nexus staging repository.  You will see something like

`    [INFO]  * Closing staging repository with ID "orgapachenifi-1011".`

So if you browse to `https://repository.apache.org/#stagingRepositories` login with your Apache committer credentials and you should see `orgapachenifi-1011`.  If you click on that you can inspect the various staged artifacts.

Validate that all the various aspects of the staged artifacts appear correct

  - Download the sources.  Do they compile cleanly?  If the result is a build does it execute?
  - Validate the hashes match.
  - Validate that the sources contain no unexpected binaries.
  - Validate the signature for the build and hashes.
  - Validate the LICENSE/NOTICE/Headers.  
  - Validate that the README is present and provides sufficient information to build and if necessary execute.
  
If all looks good then push the branch to origin `git push origin NIFI-270`

If it is intended that convenience binaries will be provided for this release then the community has requested that
a copy it be made available for reviewing of the release candidate.  The convenience binary, its hashes, and signature
 should be placed here:
  - https://dist.apache.org/repos/dist/dev/nifi

If anything isn't correct about the staged artifacts you can drop the staged repo from repository.apache.org and delete the
local tag in git.  If you also delete the local branch and clear your local maven repository under org/apache/nifi then it is
as if the release never happened.  Before doing that though try to figure out what went wrong.  So as described here you see
that you can pretty easily test the release process until you get it right.  The `mvn versions:set ` and `mvn versions:commit `
commands can come in handy to help do this so you can set versions to something clearly release test related.

Now it's time to initiate a vote within the NiFi community.  Send the vote request to `dev@nifi.apache.org`
with a subject of `[VOTE] Release Apache NiFi 0.0.1`. The following template can be used:
 
```
Hello
I am pleased to be calling this vote for the source release of Apache NiFi
nifi-0.0.1.

The source zip, including signatures, digests, etc. can be found at:
https://repository.apache.org/content/repositories/orgapachenifi-1011

The Git tag is nifi-0.0.1-RC1
The Git commit ID is 72abf18c2e045e9ef404050e2bffc9cef67d2558
https://git-wip-us.apache.org/repos/asf?p=nifi.git;a=commit;h=72abf18c2e045e9ef404050e2bffc9cef67d2558

Checksums of nifi-0.0.1-source-release.zip:
MD5: 5a580756a17b0573efa3070c70585698
SHA1: a79ff8fd0d2f81523b675e4c69a7656160ff1214

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/joewitt.asc

KEYS file available here:
https://dist.apache.org/repos/dist/release/nifi/KEYS

8 issues were closed/resolved for this release:
https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12316020&version=12329307

The vote will be open for 72 hours. 
Please download the release candidate and evaluate the necessary items including checking hashes, signatures, build from source, and test.  The please vote:

[ ] +1 Release this package as nifi-0.0.1
[ ] +0 no opinion
[ ] -1 Do not release this package because because...
```
<br/>
A release vote is majority rule.  So wait 72 hours and see if there are at least 3 binding (PMC members) +1 votes and no more negative votes than positive.
If the vote passes then send a vote result email.  Send the email to `dev@nifi.apache.org`
with a subject of `[RESULT][VOTE] Release Apache NiFi 0.0.1`.  Use a template such as:

```
Hello

The release passes with

X +1 (binding) votes
Y -1 (binding) votes

Thanks to all who helped make this release possible.

Here is the PMC vote thread: [INSERT URL OF PMC Vote Thread]
```
<br/>
Now all the voting is done and the release is good to go.

Here are the steps of the release once the release is approved:

1. Upload source-release artifacts to dist.  If the release version is 0.0.1 then upload them (zip, asc, md5, sha1) to
`https://dist.apache.org/repos/dist/release/nifi/0.0.1`

2. To produce binary convenience release build the application from the raw source in staging.  For each binary convenience artifact:  
    - Generate ascii armored detached signature by running `gpg -a -b nifi-0.0.1-bin.tar.gz`
    - Generate md5 hash summary by running `md5sum nifi-0.0.1-bin.tar.gz | awk '{ printf substr($0,0,32)}' >  nifi-0.0.1-bin.tar.gz.md5`
    - Generate sha1 hash summary by running `sha1sum nifi-0.0.1-bin.tar.gz | awk '{ printf substr($0,0,40)}' >  nifi-0.0.1-bin.tar.gz.sha1`
    - Upload the bin, asc, sha1, md5 for each binary convenience build to the same location as the source release

3.  In repository.apache.org go to the staging repository and select `release` and follow instructions on the site.

4.  Merge the release branch into master

5.  Merge the release branch into develop

6.  Update the NiFi website to point to the new download(s)

7.  Update the NiFi Web Page to indicate NEWS of the release as appropriate

8.  In Jira mark the release version as 'Released' and 'Archived' through 'version' management in the 'administration' console.

9.  Wait 24 hours then send release announcement.
    - See [here][release-announce] for an understanding of why you need to wait 24 hours
    - Then create an announcement like the one shown below addressed to 'announce@apache.org, dev@nifi..apache.org' with a reply-to of 'dev@nifi.apache.org'.  
    - The email has to be sent from an apache.org email address and should be by the release manager of the build.

```
SUBJECT:  [ANNOUNCE] Apache NiFi 0.0.2 release
BODY:
Hello

The Apache NiFi team would like to announce the release of Apache NiFi 0.0.2.

Apache NiFi is an easy to use, powerful, and reliable system to process and distribute data.  Apache NiFi was made for dataflow.  It supports highly configurable directed graphs of data routing, transformation, and system mediation logic.

More details on Apache NiFi can be found here:
http://nifi.apache.org/

The release artifacts can be downloaded from here:
http://nifi.apache.org/download.html
    
Maven artifacts have been made available here:
https://repository.apache.org/content/repositories/releases/org/apache/nifi/
     
Release notes available here:
https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12316020&version=12329373
     
Thank you
The Apache NiFi team
    
```

[quickstart-guide]: http://nifi.apache.org/development/quickstart.html
[release-manager]: http://www.apache.org/dev/release-publishing.html#release_manager
[release-announce]: http://www.apache.org/dev/release.html#release-announcements
[apache-license]: http://apache.org/licenses/LICENSE-2.0
[apache-license-apply]: http://www.apache.org/dev/apply-license.html
[apache-legal-resolve]: http://www.apache.org/legal/resolved.html
[apache-encryption]: http://www.apache.org/licenses/exports/
[apache-release-policy]: http://www.apache.org/dev/release.html
[apache-release-guide]: http://www.apache.org/dev/release-publishing
[apache-pgp]: http://www.apache.org/dev/openpgp.html
[apache-release-signing]: http://www.apache.org/dev/release-signing.html
[apache-guide-publish-maven]: http://www.apache.org/dev/publishing-maven-artifacts.html
