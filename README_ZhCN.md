<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
[<img src="https://nifi.apache.org/assets/images/apache-nifi-logo.svg" width="300" height="126" alt="Apache NiFi"/>][nifi]

<p align="center">
<a href="./README.md">English</a>
</p>

[![ci-workflow](https://github.com/apache/nifi/workflows/ci-workflow/badge.svg)](https://github.com/apache/nifi/actions)
[![Docker pulls](https://img.shields.io/docker/pulls/apache/nifi.svg)](https://hub.docker.com/r/apache/nifi/)
[![Version](https://img.shields.io/maven-central/v/org.apache.nifi/nifi-utils.svg)](https://nifi.apache.org/download.html)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://s.apache.org/nifi-community-slack)

[Apache NiFi](https://nifi.apache.org/) 是一个易于使用，功能强大且可靠的系统，用于处理和分发数据。

## 目录

- [特点](#特点)
- [要求](#要求)
- [入门](#入门)
- [帮助](#帮助)
- [文档](#文档)
- [许可](#许可)
- [受限](#受限)

## 特点

Apache NiFi 专门用于数据流。它支持数据路由，转换和系统中介逻辑的高度可配置的有向图。它的一些主要功能包括：

- 基于Web的用户界面
  - 无缝的设计，控制和监控经验
  - 多租户用户体验
- 高度可配置
  - 容忍损失与保证交付
  - 低延迟与高吞吐量
  - 动态优先级
  - 可以在运行时修改流
  - 背压
  - 扩大规模以利用全部机器功能
  - 使用零领导者聚类模型进行横向扩展
- 数据血缘
  - 从头到尾跟踪数据流
- 专为扩展而设计
  - 构建自己的处理器等
  - 实现快速开发和有效测试
- 安全
  - SSL, SSH, HTTPS, 内容加密等...
  - 可插拔的细粒度基于角色的身份验证/授权
  - 多个团队可以管理和共享流程的特定部分

## 要求
* JDK 1.8 (*NiFi可在Java 9/10/11上运行；请参阅 [NIFI-5174](https://issues.apache.org/jira/browse/NIFI-5174)*)
* Apache Maven 3.1.1 或更高版本
* Git Client (在构建过程中由“ bower”插件使用)

## 入门

- 通读 [快速入门开发指南](http://nifi.apache.org/quickstart.html).
  它将包括有关获取源代码本地副本的信息，提供有关问题跟踪的指针，并提供有关开发环境中常见问题的一些警告。
- 有关更全面的开发指南以及有关对项目做出贡献的信息，请阅读《[NiFi开发人员指南](http://nifi.apache.org/developer-guide.html)》。

 构建:
- 执行 `mvn clean install` 或并行构建执行 `mvn -T 2.0C clean install`. 在使用了几年的适度开发笔记本电脑上，后者的构建时间不到10分钟。经过大量输出之后，您最终应该会看到一条成功消息。

        laptop:nifi myuser$ mvn -T 2.0C clean install
        [INFO] Scanning for projects...
        [INFO] Inspecting build with total of 115 modules...
            ...tens of thousands of lines elided...
        [INFO] ------------------------------------------------------------------------
        [INFO] BUILD SUCCESS
        [INFO] ------------------------------------------------------------------------
        [INFO] Total time: 09:24 min (Wall Clock)
        [INFO] Finished at: 2015-04-30T00:30:36-05:00
        [INFO] Final Memory: 173M/1359M
        [INFO] ------------------------------------------------------------------------
- 执行 `mvn clean install -DskipTests` 以编译测试，但是跳过运行它们。

部署:
- 进入目录'nifi-assembly'. 在目标目录中，应该有一个构建好的nifi版本。

        laptop:nifi myuser$ cd nifi-assembly
        laptop:nifi-assembly myuser$ ls -lhd target/nifi*
        drwxr-xr-x  3 myuser  mygroup   102B Apr 30 00:29 target/nifi-1.0.0-SNAPSHOT-bin
        -rw-r--r--  1 myuser  mygroup   144M Apr 30 00:30 target/nifi-1.0.0-SNAPSHOT-bin.tar.gz
        -rw-r--r--  1 myuser  mygroup   144M Apr 30 00:30 target/nifi-1.0.0-SNAPSHOT-bin.zip

- 为了测试正在进行的开发，您可以使用存在于名为 "nifi-*version*-bin"的目录中的已经解压的内部版本, 其中 *version* 是当前项目的版本。要部署在其他位置，请使用tarball或zipfile，并在任何需要的地方解压缩它们。该发行版将在以该版本命名的公共父目录中。

        laptop:nifi-assembly myuser$ mkdir ~/example-nifi-deploy
        laptop:nifi-assembly myuser$ tar xzf target/nifi-*-bin.tar.gz -C ~/example-nifi-deploy
        laptop:nifi-assembly myuser$ ls -lh ~/example-nifi-deploy/
        total 0
        drwxr-xr-x  10 myuser  mygroup   340B Apr 30 01:06 nifi-1.0.0-SNAPSHOT

运行:
- 将目录更改为安装并运行NiFi的位置.

        laptop:~ myuser$ cd ~/example-nifi-deploy/nifi-*
        laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh start

- 浏览器中访问 http://localhost:8080/nifi/ 您应该会看到类似此屏幕截图的屏幕:
  ![image of a NiFi dataflow canvas](nifi-docs/src/main/asciidoc/images/nifi_first_launch_screenshot.png?raw=true)

- 如需有关建立第一个数据流的帮助，请参阅《[NiFi用户指南](http://nifi.apache.org/docs/nifi-docs/html/user-guide.html)》

- 如果您正在测试正在进行的开发，则可能要停止实例.

        laptop:~ myuser$ cd ~/example-nifi-deploy/nifi-*
        laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh stop

## 帮助
如有疑问，请联系我们的邮件列表: dev@nifi.apache.org
([archive](http://mail-archives.apache.org/mod_mbox/nifi-dev)).对于更多的交互式讨论，社区成员通常可以在以下位置找到：

- Apache NiFi Slack Workspace: https://apachenifi.slack.com/

  新用户可以使用以下[邀请链接](https://s.apache.org/nifi-community-slack)加入.
  
- IRC: #nifi on [irc.freenode.net](http://webchat.freenode.net/?channels=#nifi)

要提交功能请求或错误报告，请提交至 [https://issues.apache.org/jira/projects/NIFI/issues](https://issues.apache.org/jira/projects/NIFI/issues). 如果这是一个**安全漏洞报告**, 请直接发送电子邮件至[security@nifi.apache.org](mailto:security@nifi.apache.org) 并首先查看 [Apache NiFi安全漏洞列表](https://nifi.apache.org/security.html) 和 [Apache Software Foundation Security](https://www.apache.org/security/committers.html). 

## 文档

有关最新文档，请参见 http://nifi.apache.org/.

## 许可

除非另有说明，否则该软件已获得 [Apache许可，其版本为2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

根据Apache许可版本2.0（“许可”）许可；除非遵守许可，否则不得使用此文件。您可以在以下位置获得许可的副本：

http://www.apache.org/licenses/LICENSE-2.0

除非适用法律要求或以书面形式同意，否则根据“许可”分发的软件将按“原样”分发，而没有任何明示或暗示的保证或条件。有关许可下特定的语言管理权限和限制，请参阅许可。


## 受限
此发行版包括加密软件。您当前居住的国家/地区可能对加密软件的导入，拥有，使用和/或再出口到另一个国家/地区有所限制。在使用任何加密软件之前，请检查您所在国家关于导入，拥有，使用和再出口加密软件的法律，法规和政策，以查看是否允许这样做。有关更多信息，请参见http://www.wassenaar.org/ 。

美国政府商务部工业和安全局（BIS）已将此软件归类为出口商品控制号（ECCN）5D002.C.1，其中包括使用或执行具有非对称算法的加密功能的信息安全软件。此Apache Software Foundation分发的形式和方式使其可以根据许可例外ENC技术软件无限制（TSU）例外（请参阅BIS出口管理条例，第740.13节）获得对象代码和源代码的出口资格。

下面提供了有关随附的加密软件的更多详细信息：

Apache NiFi使用BouncyCastle，JCraft Inc.和内置的Java加密库来实现SSL，SSH和敏感配置参数的保护。参见 
http://bouncycastle.org/about.html 
http://www.jcraft.com/c-info.html http://www.oracle.com/us/products/export/export-regulations-345813.html
有关这些库加密功能的每一个的更多详细信息。

[nifi]: https://nifi.apache.org/
[logo]: https://nifi.apache.org/assets/images/apache-nifi-logo.svg
