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

import scala.language.postfixOps
import scala.math.sqrt
import scala.reflect.ClassTag


import org.apache.spark.Logging
import org.apache.spark.graphx._

/**
 * HTIS algorithm implementation.
 *
 */
object HITS extends Logging {


   /**
   * Run the HITS algorithm for a fixed number of iterations returning a graph
   * with vertex attributes containing the hub and authority scores
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HITS hub and authority scores
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph containing vertices with their corresponding hub and authority
   * scores as a vertex attribute (hub,auth) and edges with the same initial edge attribute
   */
  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), ED] =
  {
    // Initialize hub and authority score of all vertices in hitsGraph to 1.0
    var hitsGraph = graph.mapVertices( (id, attr) => (1.0, 1.0) )

    var iteration = 0
    while (iteration < numIter) {
      // Perform authority update rule
      val newAuths = hitsGraph.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr._1), _+_)
      val authNorm = sqrt(newAuths.map(elem => elem._2*elem._2).reduce((a,b) => a+b))
      hitsGraph = hitsGraph.joinVertices(newAuths) {
        (_, oldScores, newAuth) => (oldScores._1, newAuth/authNorm)
      }
      
      // Perform hub update rule
      val newHubs = hitsGraph.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.dstAttr._2), _+_)
      val hubNorm = sqrt(newHubs.map(elem => elem._2*elem._2).reduce((a, b) => a + b))
      hitsGraph = hitsGraph.joinVertices(newHubs) {
        (_, oldScores, newHub) => (newHub/hubNorm, oldScores._2)
      }

      iteration += 1
    }

    hitsGraph
  }
}