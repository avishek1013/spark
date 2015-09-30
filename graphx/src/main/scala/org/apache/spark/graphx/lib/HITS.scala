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
 * HITS algorithm implementation. The algorithm calculates a hub and authority score for each
 * vertex in a [[Graph]] by iteratively updating the authority value for all vertices and then
 * updating the hub value for all vertices. This is repeated until a set number of iterations
 * have been passed.
 *
 * Psuedocode for the algorithm is as follows:
 *
 * {{{
 * graph := set of vertices and edges
 * // Initialize hub and authority scores
 * for each vertex v in graph do
 *   v.hub = 1.0
 *   v.auth = 0.0
 * 
 * // Loop numIter times
 * iter = 0
 * while iter < numIter do
 *   authNorm = 0.0
 *   // For every vertex, set the authority score to the sum of incoming hub scores
 *   for each vertex v in graph do
 *     v.auth = 0.0
 *     for each vertex u in v.inNbrs do
 *       v.auth += u.hub
 *     authNorm += v.auth * v.auth
 *   
 *   // Normalize each authority score
 *   authNorm = sqrt(authNorm)
 *   for each vertex v in graph do
 *     v.auth /= authNorm
 * 
 *   hubNorm = 0.0
 *   // For every vertex, set the hub score to the sum of outgoing authority scores
 *   for each vertex v in graph do
 *     v.hub = 0.0
 *     for each vertex u in v.outNbrs do
 *       v.hub += u.auth
 *     hubNorm += v.hub * v.hub
 * 
 *   // Normalize each hub score
 *   hubNorm = sqrt(hubNorm)
 *   for each vertex v in graph do
 *     v.hub /= hubNorm
 *
 *   iter += 1
 * }}}
 */
object HITS extends Logging {

  /**
   * Run the HITS algorithm for a fixed number of iterations, returning a [[Graph]]
   * with vertex attributes containing the hub and authority scores. The hub and authority
   * scores range from 0.0 to 1.0. A hub score of 0.0 indicates that the vertex is not a hub
   * (has no outgoing edges) and a score of 1.0 indicates that the vertex is the only hub (all 
   * edges in the graph begin at the vertex). An authority score of 0.0 indicates that the 
   * vertex is not an authority (has no incoming edges) and a score of 1.0 indicates that the 
   * vertex is the only authority (all edges in the graph end at the vertex).
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HITS hub and authority scores
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph containing vertices with their corresponding hub and authority
   *         scores as a vertex attribute (hub, auth) and edges with Unit as the edge attribute
   */
  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), Unit] =
  {
    // Initialize hub and authority score of all vertices
    var hitsGraph = graph.mapVertices( (vid, attr) => (1.0, 0.0) ).cache()

    // Repeat numIter times
    var iteration = 0
    while (iteration < numIter) {

      // Perform authority update rule and normalize
      val newAuths = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._1), _ + _, TripletFields.Src)
      val authNorm = sqrt(newAuths.map( elem => elem._2 * elem._2 ).sum())
      hitsGraph = hitsGraph.joinVertices(newAuths) {
        (_, oldScores, newAuth) => (oldScores._1, newAuth / authNorm)
      }
      
      // For the first pass, we need to reset the hub values of all vertices. This ensures that 
      // vertices that do not point to other vertices are properly given a hub score of 0.0
      if (iteration == 0) {
        hitsGraph = hitsGraph.mapVertices( (vid, attr) => (0.0, attr._2) )
      }

      // Perform hub update rule and normalize
      val newHubs = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._2), _ + _, TripletFields.Dst)
      val hubNorm = sqrt(newHubs.map(elem => elem._2 * elem._2).sum())
      hitsGraph = hitsGraph.joinVertices(newHubs) {
        (_, oldScores, newHub) => (newHub / hubNorm, oldScores._2)
      }

      iteration += 1
    }

    // Discard edge attribute and return graph
    hitsGraph.mapEdges( (edge) => Unit )
  }
}
