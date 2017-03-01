;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(import org.apache.commons.io.IOUtils)
(import [java.io BufferedReader InputStreamReader])

(let [flowFile (.get session)]
  (if (some? flowFile)
     (do

       (let [ inReader (BufferedReader. (InputStreamReader. (.read session flowFile) "UTF-8"))
              header (.split (.readLine inReader) ",")]

         (.transfer session
           (.write session
             (.putAttribute session
               (.putAttribute session (.create session) "selected.columns" (str (aget header 1) "," (aget header 2)))
                 "filename" "split_cols.txt"
             )
             (reify OutputStreamCallback
               (process [this outputStream]
                 (loop []
                   (let [line (.readLine inReader)]
                     (if (some? line)
                       (let [cols (.split line ",")]
                         (.write outputStream (.getBytes (str (aget cols 3) ", " (aget cols 2) "\n")))
                         (recur)
                       )
                     )
                   )
                 )
               )
             )
           ) REL_SUCCESS
         )
         (.close inReader)
       )
       (.remove session flowFile)
     )
   )
)