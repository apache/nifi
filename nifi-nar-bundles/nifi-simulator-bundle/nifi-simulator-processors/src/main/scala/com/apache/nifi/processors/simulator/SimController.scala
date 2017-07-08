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

package com.apache.nifi.processors.simulator

import java.io.File
import be.cetic.tsimulus.config.Configuration
import be.cetic.tsimulus.timeseries._
import com.github.nscala_time.time.Imports._
import spray.json._
import scala.io.Source

object SimController
{
  /**
    * Returns a point in time value for all exported values in the configuration file
    * @param ts The value in time to generate data for
    * @return a scala Iterable of Tuples in the form of URI, Timestamp, value
    */
  def getTimeValue(ts: Map[String, (TimeSeries[Any], _root_.com.github.nscala_time.time.Imports.Duration)], genTime: LocalDateTime) : Iterable[(String,LocalDateTime,AnyRef)] =
  {
    ts.map(series =>
    {
      val values = series._2._1
      val time = genTime
      val data = values.compute(time)
      new Tuple3[String,LocalDateTime,AnyRef](series._1, time, data)
    })

  }

  /**
    * Wrapped scala method to be able to be accessed from Java
    * @param filePath The path to the configuration file
    * @return The configuration object parsed from the file
    */
  def getConfiguration(filePath: String): Configuration = {
    val content = Source .fromFile(new File(filePath))
      .getLines()
      .mkString("\n")

    Configuration(content.parseJson);
  }

}
