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


[![ci-workflow](https://github.com/apache/nifi/workflows/ci-workflow/badge.svg)](https://github.com/apache/nifi/actions/workflows/ci-workflow.yml)
[![system-tests](https://github.com/apache/nifi/workflows/system-tests/badge.svg)](https://github.com/apache/nifi/actions/workflows/system-tests.yml)
[![Docker pulls](https://img.shields.io/docker/pulls/apache/nifi.svg)](https://hub.docker.com/r/apache/nifi/)
[![Version](https://img.shields.io/maven-central/v/org.apache.nifi/nifi-utils.svg)](https://nifi.apache.org/download.html)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://s.apache.org/nifi-community-slack)

[Apache NiFi](https://nifi.apache.org/) 是一个易于使用，功能强大且可靠的系统，用于处理和分发数据。

## 目录

- [特点](#特点)
- [要求](#要求)
- [入门](#入门)
- [MiNiFi 子项目](#MiNiFi 子项目)
- [注册表子项目](#注册表子项目)
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
**最低要求**
* JDK 1.8 (*NiFi可在Java 9/10/11上运行；请参阅 [NIFI-5174](https://issues.apache.org/jira/browse/NIFI-5174)*)
* Apache Maven 3.6.0 或更高版本
* Git Client (在构建过程中由“ bower”插件使用)

## 入门

- 通读 [快速入门开发指南](http://nifi.apache.org/quickstart.html).
  它将包括有关获取源代码本地副本的信息，提供有关问题跟踪的指针，并提供有关开发环境中常见问题的一些警告。
- 有关更全面的开发指南以及有关对项目做出贡献的信息，请阅读《[NiFi开发人员指南](http://nifi.apache.org/developer-guide.html)》。

### 构建
- 执行 `mvn clean install` 或并行构建执行 `mvn -T 2.0C clean install`. 在当下较好配置的机器上，构建时间大约为10分钟。

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

### 部署
- 进入目录'nifi-assembly'. 在目标目录中，应该有一个构建好的nifi版本。

        laptop:nifi myuser$ cd nifi-assembly
        laptop:nifi-assembly myuser$ ls -lhd target/nifi*
        drwxr-xr-x  3 myuser  mygroup   102B Apr 30 00:29 target/nifi-1.0.0-SNAPSHOT-bin
        -rw-r--r--  1 myuser  mygroup   144M Apr 30 00:30 target/nifi-1.0.0-SNAPSHOT-bin.tar.gz
        -rw-r--r--  1 myuser  mygroup   144M Apr 30 00:30 target/nifi-1.0.0-SNAPSHOT-bin.zip

将nifi-VERSION-bin.tar.gz或复制nifi-VERSION-bin.zip到单独的部署目录。提取发行版将创建一个以版本命名的新目录。

```
laptop:nifi-assembly myuser$ mkdir ~/example-nifi-deploy
laptop:nifi-assembly myuser$ tar xzf target/nifi-*-bin.tar.gz -C ~/example-nifi-deploy
laptop:nifi-assembly myuser$ ls -lh ~/example-nifi-deploy/
total 0
drwxr-xr-x  10 myuser  mygroup   340B Apr 30 01:06 nifi-1.0.0-SNAPSHOT
```

### 开始
将目录更改为安装并运行NiFi的位置

```
laptop:~ myuser$ cd ~/example-nifi-deploy/nifi-*
laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh start
```
运行bin/nifi.sh start在后台启动 NiFi 并。--wait-for-init与可选的超时（以秒为单位）一起使用，以在退出之前等待完全启动。

```
laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh start --wait-for-init 120
```
 
### 认证

默认配置在启动时生成随机用户名和密码。NiFi 将生成的凭据写入位于logs/nifi-app.logNiFi 安装目录下的应用程序日志。

以下命令可用于在日志中查找生成的凭据：

```
laptop:nifi-1.0.0-SNAPSHOT myuser$ grep Generated logs/nifi-app*log
```

NiFi 记录生成的凭据如下：

    Generated Username [USERNAME]
    Generated Password [PASSWORD]

USERNAME是36个字符构成的无规UUID。
PASSWORD是32个字符组成的随机串。
生成的凭据存储在conf/login-identity-providers.xml。请将这些凭据记录在安全位置以访问 NiFi。
也可以使用以下命令将随机用户名和密码替换为自定义凭据：

    ./bin/nifi.sh set-single-user-credentials <username> <password>
  
### 运行

在 Web 浏览器中打开以下链接以访问 NiFi：https://localhost:8443/nifi

Web 浏览器将显示一条警告消息，表明由于NiFi初始化期间生成的自签名证书存在潜在的安全风险。接受潜在的安全风险并继续加载接口是初始开发安装的一个选项。生产部署应提供来自受信任证书颁发机构的证书并更新 NiFi 密钥库和信任库配置。

接受自签名证书后访问 NiFi 将显示登录界面。
![NiFi Login Screen](nifi-docs/src/main/asciidoc/images/nifi-login.png?raw=true)

使用生成的凭据，在User字段中输入生成的用户名和生成的密码Password，然后选择LOG IN访问系统。
![NiFi Flow Authenticated Screen](nifi-docs/src/main/asciidoc/images/nifi-flow-authenticated.png?raw=true)

### 配置

该 [NiFi 用户指南](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html) 介绍了如何构建一个数据流。

### 停止

运行以下命令停止 NiFi：

    laptop:~ myuser$ cd ~/example-nifi-deploy/nifi-*
    laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh stop

## MiNiFi 子项目

MiNiFi 是 Apache NiFi 的子项目。它是一种补充数据收集方法，补充了 [NiFi](https://nifi.apache.org/) 在数据流管理中的核心原则，专注于在其创建源头收集数据。

MiNiFi 的具体特点包括：
- 体积小、重量轻
- 代理的集中管理
- 数据来源的生成
- 与 NiFi 集成以进行后续数据流管理和完整的信息监管链

MiNiFi 的角色应该从代理的角度来看待，该代理直接在源传感器、系统或服务器附近或直接与其相邻。

运行:
- 进入目录“minifi-assembly”。在目标目录中，应该有一个 minifi 的构建包。

        $ cd minifi-assembly
        $ ls -lhd target/minifi*
        drwxr-xr-x  3 user  staff   102B Jul  6 13:07 minifi-1.14.0-SNAPSHOT-bin
        -rw-r--r--  1 user  staff    39M Jul  6 13:07 minifi-1.14.0-SNAPSHOT-bin.tar.gz
        -rw-r--r--  1 user  staff    39M Jul  6 13:07 minifi-1.14.0-SNAPSHOT-bin.zip

- 为了测试正在进行的开发，您可以使用名为 "minifi-*version*-bin"的目录中存在的已解压构建，其中 *version* 是当前项目版本。要部署到另一个位置，请使用 tarball 或 zipfile 并在您喜欢的任何位置解压缩它们。分发将位于以版本命名的公共父目录中。

        $ mkdir ~/example-minifi-deploy
        $ tar xzf target/minifi-*-bin.tar.gz -C ~/example-minifi-deploy
        $ ls -lh ~/example-minifi-deploy/
        total 0
        drwxr-xr-x  10 user  staff   340B Jul 6 01:06 minifi-1.14.0-SNAPSHOT

运行 MiNiFi:
- 将目录更改为安装 MiNiFi 的位置并运行它。

        $ cd ~/example-minifi-deploy/minifi-*
        $ ./bin/minifi.sh start

- 查看日志文件夹中的日志 $ tail -F ~/example-minifi-deploy/logs/minifi-app.log

- 如需帮助构建您的第一个数据流并将数据发送到 NiFi 实例，请参阅位于 docs 文件夹中的系统管理指南或使用 minifi-toolkit，它有助于将 NiFi 模板调整为 MiNiFi YAML 配置文件格式。

- 如果您正在测试正在进行的开发，您可能希望停止您的实例。

        $ cd ~/example-minifi-deploy/minifi-*
        $ ./bin/minifi.sh stop

### Docker 构建

构建:
- 运行完整的 NiFi 构建（有关说明，请参见上文）。然后从 minifi/ 子目录执行 `mvn -P docker clean install`.  这将运行完整的构建，基于它创建一个 docker 镜像，并运行 docker-compose 集成测试。成功完成后，您应该有一个 apache minifi:${minifi.version} 映像，可以使用以下命令启动它（将 ${minifi.version} 替换为您分支的当前 maven 版本）：
```
docker run -d -v YOUR_CONFIG.YML:/opt/minifi/minifi-${minifi.version}/conf/config.yml apacheminifi:${minifi.version}
```

## Registry 子项目

Registry（Apache NiFi 的一个子项目）是一个补充应用程序，它为跨一个或多个 NiFi 和/或 MiNiFi 实例的共享资源的存储和管理提供了一个中央位置。

### 开始 Registry

1) 构建 NiFi (参阅 [NiFi入门](#getting-started) )
    
或者
    
仅构建 Registry 子项目:

    cd nifi/nifi-registry
    mvn clean install

    
如果您希望启用样式和许可证检查，请指定 contrib-check 配置文件：

    mvn clean install -Pcontrib-check


2) 启动 Registry

    cd nifi-registry/nifi-registry-assembly/target/nifi-registry-<VERSION>-bin/nifi-registry-<VERSION>/
    ./bin/nifi-registry.sh start

注意：应用程序 Web 服务器在可访问之前可能需要一段时间才能加载。   

3) 访问应用程序 Web UI
 
使用默认设置，应用程序 UI 将在 [http://localhost:18080/nifi-registry](http://localhost:18080/nifi-registry) 
   
4) 访问应用程序 REST API

如果您希望针对应用程序 REST API 进行测试，您可以直接访问 REST API。使用默认设置，REST API 的基本 URL 将为`http://localhost:18080/nifi-registry-api`. 用于测试 REST API 的 UI 将在[http://localhost:18080/nifi-registry-api/swagger/ui.html](http://localhost:18080/nifi-registry-api/swagger/ui.html) 

5) 访问应用程序日志

日志将存储在 `logs/nifi-registry-app.log`

### 数据库测试

为了确保 NiFi Registry 针对不同的关系数据库正确工作，可以利用[Testcontainers 框架](https://www.testcontainers.org/)针对不同的数据库运行现有的集成测试。

Spring 配置文件用于控制可用于 Spring 应用程序上下文的 DataSource 工厂。提供的数据源工厂使用 Testcontainers 框架为给定的数据库启动 Docker 容器并创建相应的数据源。如果未指定配置文件，则默认情况下将使用 H2 数据源，并且不需要 Docker 容器。

假设 Docker 正在运行构建的系统上运行，那么可以运行以下命令：


| 目标数据库 | 构建命令 | 
| --------------- | ------------- |
| 全部支持 | `mvn verify -Ptest-all-dbs` |
| H2 (默认)    | `mvn verify` |
| PostgreSQL 9.x  | `mvn verify -Dspring.profiles.active=postgres` | 
| PostgreSQL 10.x | `mvn verify -Dspring.profiles.active=postgres-10` | 
| MySQL 5.6       | `mvn verify -Pcontrib-check -Dspring.profiles.active=mysql-56` |
| MySQL 5.7       | `mvn verify -Pcontrib-check -Dspring.profiles.active=mysql-57` |
| MySQL 8         | `mvn verify -Pcontrib-check -Dspring.profiles.active=mysql-8`  |
      
 当 Testcontainer 配置文件之一被激活时，测试输出应显示指示容器已启动的日志，例如：
 
    2019-05-15 16:14:45.078  INFO 66091 --- [           main] 🐳 [mysql:5.7]                           : Creating container for image: mysql:5.7
    2019-05-15 16:14:45.145  INFO 66091 --- [           main] o.t.utility.RegistryAuthLocator          : Credentials not found for host (index.docker.io) when using credential helper/store (docker-credential-osxkeychain)
    2019-05-15 16:14:45.646  INFO 66091 --- [           main] 🐳 [mysql:5.7]                           : Starting container with ID: ca85c8c5a1990d2a898fad04c5897ddcdb3a9405e695cc11259f50f2ebe67c5f
    2019-05-15 16:14:46.437  INFO 66091 --- [           main] 🐳 [mysql:5.7]                           : Container mysql:5.7 is starting: ca85c8c5a1990d2a898fad04c5897ddcdb3a9405e695cc11259f50f2ebe67c5f
    2019-05-15 16:14:46.479  INFO 66091 --- [           main] 🐳 [mysql:5.7]                           : Waiting for database connection to become available at jdbc:mysql://localhost:33051/test?useSSL=false&allowPublicKeyRetrieval=true using query 'SELECT 1'

Flyway 连接还应指明给定的数据库：

    2019-05-15 16:15:02.114  INFO 66091 --- [           main] o.a.n.r.db.CustomFlywayConfiguration     : Determined database type is MYSQL
    2019-05-15 16:15:02.115  INFO 66091 --- [           main] o.a.n.r.db.CustomFlywayConfiguration     : Setting migration locations to [classpath:db/migration/common, classpath:db/migration/mysql]
    2019-05-15 16:15:02.373  INFO 66091 --- [           main] o.a.n.r.d.CustomFlywayMigrationStrategy  : First time initializing database...
    2019-05-15 16:15:02.380  INFO 66091 --- [           main] o.f.c.internal.license.VersionPrinter    : Flyway Community Edition 5.2.1 by Boxfuse
    2019-05-15 16:15:02.403  INFO 66091 --- [           main] o.f.c.internal.database.DatabaseFactory  : Database: jdbc:mysql://localhost:33051/test (MySQL 5.7)

有关可用数据源工厂的完整列表，请参阅`nifi-registry-test`模块。

## 帮助
如有疑问，请联系我们的邮件列表: dev@nifi.apache.org
([存档](http://mail-archives.apache.org/mod_mbox/nifi-dev)).对于更多的交互式讨论，社区成员通常可以在以下位置找到：

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
- https://bouncycastle.org/about.html
- http://www.jcraft.com/c-info.html
- https://www.oracle.com/corporate/security-practices/corporate/governance/global-trade-compliance.html
有关这些库加密功能的每一个的更多详细信息。

[nifi]: https://nifi.apache.org/
[logo]: https://nifi.apache.org/assets/images/apache-nifi-logo.svg
