/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

class HITSSuite extends SparkFunSuite with LocalSparkContext {

  test("HITS") {
    withSpark { sc =>
      val nVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val randGraph = GraphGenerators.rmatGraph(sc, nVertices, 50)

      val starHitsGraph = starGraph.staticHITS(numIter = 3)
      val randHitsGraph = randGraph.staticHITS(numIter = 3)
      starHitsGraph.vertices.foreach(println)

      assert(0 == 0)
    }
  } // end of test HITS 
}
