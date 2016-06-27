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
# Apache NiFi [![Build Status](https://travis-ci.org/apache/nifi.svg?branch=master)](https://travis-ci.org/apache/nifi)

Apache NiFi es facil de usar, poderoso y un sistema de confianza para procesar y distribuir datos.

## Tabla de contentos

- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Getting Help](#getting-help)
- [Documentation](#documentation)
- [License](#license)
- [Export Control] (#export-control)

## Características

Apache NiFi fue creado para flujo de datos. Apoya configuracion directamente para las graficas con  la ruta de los datos, transformacion y logica de el sistema.
Algunos de las Características incluye:

- Interfaz basado en un web.
  - Experencia sin costura para disenos, control y monitoriar.
- Alta configuracion.
  - Tolerante a las perdidas vs entrega garantisada
  - Baja latencia vs alta produccion
  - priorización dinamica
  - Los flows pueden sen modificado cuando corre.
  - Menos presión
- Origen de la data
  - Seguir  flujo de datos desde el principio y al final.
- Designado para extensiones
  - Crea tu propio procesor y mas.
  - Enables rapid development and effective testing
- Segurida
  - SSL, SSH, HTTPS,  encriptación de el contenido etc...
  - Pluggable role-based authentication/authorization

## Requisitos:
* JDK 1.7 o ultima version
* Apache Maven 3.1.0 o ultima version

## Como Empesar:

- lee el  [quickstart guide for development](http://nifi.apache.org/quickstart.html).
  Va a incluir informacion en como cojer una copia local de el programa, como dar consejos para problemas, y dar avisos con problemas comunes cuando estas programando.

- Para mas detalles en programar y informacion para contribuir para el projecto lea. [NiFi Developer's Guide](http://nifi.apache.org/developer-guide.html).

Para crear:
- Ejecuta `mvn clean install` o  crear paralelo ejecuta `mvn -T 2.0C clean install`. En una laptop modesta de unos cuantos
años se tarda como diez minutos. Despues de tantos mensajes vas a ver otro mensaje de éxito.

        laptop:nifi fhampton$ mvn -T 2.0C clean install
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

Para Utilizar:
- Cambia de directoria a 'nifi-assembly'. En directoria q escojistes, deveria ver una versión de nifi.

        laptop:nifi fhampton$ cd nifi-assembly
        laptop:nifi-assembly fhampton$ ls -lhd target/nifi*
        drwxr-xr-x  3 fhampton  staff   102B Apr 30 00:29 target/nifi-0.1.0-SNAPSHOT-bin
        -rw-r--r--  1 fhampton  staff   144M Apr 30 00:30 target/nifi-0.1.0-SNAPSHOT-bin.tar.gz
        -rw-r--r--  1 fhampton  staff   144M Apr 30 00:30 target/nifi-0.1.0-SNAPSHOT-bin.zip

- Para testiar el desarrollo puedes usar los build que estan presente en el directorio que se llaman
  "nifi-*version*-bin", donde *version* es la version disponible en el projecto. Para implementar en otro lugar puedes usar
  el tar o el zip y sacarlos donde quieras. La distribución va a estar en una directoria común a el padre con el nombre de la version.


        laptop:nifi-assembly fhampton$ mkdir ~/example-nifi-deploy
        laptop:nifi-assembly fhampton$ tar xzf target/nifi-*-bin.tar.gz -C ~/example-nifi-deploy
        laptop:nifi-assembly fhampton$ ls -lh ~/example-nifi-deploy/
        total 0
        drwxr-xr-x  10 fhampton  staff   340B Apr 30 01:06 nifi-0.1.0-SNAPSHOT

Para corre:
- Cambia la directoria a el sitio donde instalastes a NIFI y corelo.

        laptop:~ fhampton$ cd ~/example-nifi-deploy/nifi-*
        laptop:nifi-0.1.0-SNAPSHOT fhampton$ ./bin/nifi.sh start

- Vete a tu browser a http://localhost:8080/nifi/ y deverias ver una pantalla como esta screenshot:
  ![image of a NiFi dataflow canvas](nifi-docs/src/main/asciidoc/images/nifi_first_launch_screenshot.png?raw=true)

- Para ayuda en crear tu primer flujo de datos por favor ve [NiFi User Guide](http://nifi.apache.org/docs/nifi-docs/html/user-guide.html)

- Si sigues testiando If you are testing ongoing development, you will likely want to stop your instance.

        laptop:~ fhampton$ cd ~/example-nifi-deploy/nifi-*
        laptop:nifi-0.1.0-SNAPSHOT fhampton$ ./bin/nifi.sh stop

## Conseguir Ayuda
Si tienes preguntas puedes mandar un mensaje a: dev@nifi.apache.org
([archive](http://mail-archives.apache.org/mod_mbox/nifi-dev)).
Tambien estamos a veces en IRC: #nifi on
[irc.freenode.net](http://webchat.freenode.net/?channels=#nifi).

## documentación

  para ver la ultima version de la documentación ve a http://nifi.apache.org/ 

## License

Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Export Control

This distribution includes cryptographic software. The country in which you
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any
encryption software, please check your country's laws, regulations and
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and Security
(BIS), has classified this software as Export Commodity Control Number (ECCN)
5D002.C.1, which includes information security software using or performing
cryptographic functions with asymmetric algorithms. The form and manner of this
Apache Software Foundation distribution makes it eligible for export under the
License Exception ENC Technology Software Unrestricted (TSU) exception (see the
BIS Export Administration Regulations, Section 740.13) for both object code and
source code.

The following provides more details on the included cryptographic software:

Apache NiFi uses BouncyCastle, Jasypt, JCraft Inc., and the built-in
java cryptography libraries for SSL, SSH, and the protection
of sensitive configuration parameters. See
http://bouncycastle.org/about.html
http://www.jasypt.org/faq.html
http://jcraft.com/c-info.html
http://www.oracle.com/us/products/export/export-regulations-345813.html
for more details on each of these libraries cryptography features.
