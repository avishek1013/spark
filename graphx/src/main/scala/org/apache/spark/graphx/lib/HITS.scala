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

import scala.reflect.ClassTag
import scala.language.postfixOps

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
    graph.triplets.foreach(println)
    println
    // Initialize hub and authority score of all vertices in hitsGraph to 1.0
    var hitsGraph = graph.mapVertices( (id, attr) => (1.0, 1.0) )
    hitsGraph.vertices.foreach(println)
    println

    var iteration = 0
    while (iteration < numIter) {
      // Peform authority update rule
      val newAuths = hitsGraph.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr._1), _ + _)
      
      hitsGraph = hitsGraph.joinVertices(newAuths) {(_, oldScores, newAuth) => (oldScores._1, newAuth)}
      
      println(authNorm)
      hitsGraph.vertices.foreach(println)
      println
      
      // Perform hub update rule
      val newHubs = hitsGraph.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.dstAttr._2), _ + _)
      hitsGraph = hitsGraph.joinVertices(newHubs) {(_, oldScores, newHub) => (newHub, oldScores._2)}
      hitsGraph.vertices.foreach(println)
      println

      iteration += 1
    }

    hitsGraph
  }
}