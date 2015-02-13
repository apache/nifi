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
# About
[Apache NiFi project] (http://nifi.incubator.apache.org).

## Getting Started

The site is built using [grunt][] task runner, [bower][] package manager for
the web, and [npm][] package manager for node. npm is used to manage the
node modules required for building this site. bower is used to manage the 
front end packages that the site will depend on.

Both grunt and bower can be installed via npm once it is installed. 

```bash
sudo npm install -g grunt-cli
sudo npm install -g bower
```

To download the front end packages required for this site run the following
command from the nifi-site directory.

```bash
bower install
```

To install the node modules required to build this site run the following
command from the nifi-site directory.

```bash
npm install
```

The site is built using [foundation][] a responsive front end framework. 
Consequently, the site is using [sass][] and [compass][] for CSS pre-processing.
This will also require ruby to be installed along with sass and compass. Both
sass and compass can be installed via ruby once it is installed.

```bash
gem install compass
```

[grunt]: http://gruntjs.com/
[bower]: http://bower.io/
[npm]: http://www.npmjs.com/
[foundation]: http://foundation.zurb.com/
[sass]: http://sass-lang.com/
[compass]: http://compass-style.org/

## Grunt Tasks

To build the site run the default grunt task. This will assemble the site and 
place the resulting site in the dist folder.

```bash
grunt
```

If developing new content/features for the site, it may be helpful to run
the dev task which will build the site and continue to watch the source
files for changes. Any changes will cause the site to be rebuilt.

```bash
grunt dev
```

## Application Style Properties

### Font
- Oswald: http://www.google.com/fonts/specimen/Oswald

### Colors
- 'ni':  #7A96A4
- 'fi':  #0F3541
- Toolbox:  #7299AC
- Toolbar:  #A0BAC7
- Flow Status: #D9E4E8
- Utilities: #81A0B4

## Technologies & Resources
- HTML5
- CSS3
- Foundation
- jQuery
- Modernizr
- Google Fonts API
- Web Font Loader
- FontAwesome

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


