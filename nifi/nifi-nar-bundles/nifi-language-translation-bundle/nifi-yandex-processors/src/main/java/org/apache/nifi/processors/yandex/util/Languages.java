/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.yandex.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Languages {
	private static final Map<String, String> languageAbbreviationMap = new HashMap<>();
	
	static {
		languageAbbreviationMap.put("ar", "arabic");
		languageAbbreviationMap.put("az", "azerbaijani");
		languageAbbreviationMap.put("be", "belarusian");
		languageAbbreviationMap.put("bg", "bulgarian");
		languageAbbreviationMap.put("bs", "bosnian");
		languageAbbreviationMap.put("ca", "catalan");
		languageAbbreviationMap.put("cs", "czech");
		languageAbbreviationMap.put("da", "danish");
		languageAbbreviationMap.put("de", "german");
		languageAbbreviationMap.put("el", "greek");
		languageAbbreviationMap.put("en", "english");
		languageAbbreviationMap.put("es", "spanish");
		languageAbbreviationMap.put("et", "estonian");
		languageAbbreviationMap.put("fi", "finnish");
		languageAbbreviationMap.put("fr", "french");
		languageAbbreviationMap.put("he", "hebrew");
		languageAbbreviationMap.put("hr", "croatian");
		languageAbbreviationMap.put("hu", "hungarian");
		languageAbbreviationMap.put("hy", "armenian");
		languageAbbreviationMap.put("id", "indonesian");
		languageAbbreviationMap.put("is", "icelandic");
		languageAbbreviationMap.put("it", "italian");
		languageAbbreviationMap.put("ja", "japanese");
		languageAbbreviationMap.put("ka", "georgian");
		languageAbbreviationMap.put("ko", "korean");
		languageAbbreviationMap.put("lt", "lithuanian");
		languageAbbreviationMap.put("lv", "latvian");
		languageAbbreviationMap.put("mk", "macedonian");
		languageAbbreviationMap.put("ms", "malay");
		languageAbbreviationMap.put("mt", "maltese");
		languageAbbreviationMap.put("nl", "dutch");
		languageAbbreviationMap.put("no", "norwegian");
		languageAbbreviationMap.put("pl", "polish");
		languageAbbreviationMap.put("pt", "portuguese");
		languageAbbreviationMap.put("ro", "romanian");
		languageAbbreviationMap.put("ru", "russian");
		languageAbbreviationMap.put("sk", "slovak");
		languageAbbreviationMap.put("sl", "slovenian");
		languageAbbreviationMap.put("sq", "albanian");
		languageAbbreviationMap.put("sr", "serbian");
		languageAbbreviationMap.put("sv", "swedish");
		languageAbbreviationMap.put("th", "thai");
		languageAbbreviationMap.put("tr", "turkish");
		languageAbbreviationMap.put("uk", "ukrainian");
		languageAbbreviationMap.put("vi", "vietnamese");
		languageAbbreviationMap.put("zh", "chinese");
		
		final Map<String, String> reverseMap = new HashMap<>();
		for ( final Map.Entry<String, String> entry : languageAbbreviationMap.entrySet() ) {
			reverseMap.put(entry.getValue(), entry.getKey());
		}
		
		languageAbbreviationMap.putAll(reverseMap);
	}
	
	
	public static Map<String, String> getLanguageMap() {
		return Collections.unmodifiableMap(languageAbbreviationMap);
	}
}
