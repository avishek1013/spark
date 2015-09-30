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

import scala.math.sqrt

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators


object StarHITS {
  def apply(nVertices: Int): Seq[(VertexId, (Double, Double))] = {
    var hubAuth = Array.fill(nVertices){ (1.0 / sqrt(nVertices - 1.0), 0.0) }
    hubAuth(0) = (0.0, 1.0)
    (0L until nVertices).zip(hubAuth)
  }
}

object ChainHITS {
  def apply(nVertices: Int): Seq[(VertexId, (Double, Double))] = {
    val score = 1.0 / sqrt(nVertices - 1.0)
    var hubAuth = Array.fill(nVertices){ (score, score) }
    hubAuth(0) = (score, 0.0)
    hubAuth(nVertices - 1) = (0.0, score)
    (0L until nVertices).zip(hubAuth)
  }
}

object GridHITS {
  def apply(nRows: Int, nCols: Int, nIter: Int): Seq[(VertexId, (Double, Double))] = {
    // Arrays to containing incoming and outgoing neighbors
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r, c)
      if (r + 1 < nRows) {
        inNbrs(sub2ind(r + 1, c)) += ind
        outNbrs(ind) += sub2ind(r + 1, c)
      }
      if (c + 1 < nCols) {
        inNbrs(sub2ind(r, c + 1)) += ind
        outNbrs(ind) += sub2ind(r, c + 1)
      }
    }

    // Initialize hub and authority scores
    var hubScores: Array[Double] = Array.fill(nRows * nCols)(1.0)
    var authScores: Array[Double] = Array.fill(nRows * nCols)(1.0)
    
    // Run nIter times
    for (iter <- 0 until nIter) {
      // Update authority score and normalize
      var authNorm = 0.0
      for (ind <- 0 until (nRows * nCols)) {
        authScores(ind) = 0.0
        authScores(ind) += inNbrs(ind).map( nbr => hubScores(nbr) ).sum
        authNorm += authScores(ind) * authScores(ind) 
      }
      authScores = authScores.map( num => num / sqrt(authNorm) )

      // Update hub scores and normalize
      var hubNorm = 0.0
      for (ind <- 0 until (nRows * nCols)) {
        hubScores(ind) = 0.0
        hubScores(ind) += outNbrs(ind).map( nbr => authScores(nbr) ).sum
        hubNorm += hubScores(ind) * hubScores(ind)
      }
      hubScores = hubScores.map( num => num / sqrt(hubNorm) )
    }

    (0L until (nRows * nCols)).zip(hubScores.zip(authScores))
  }
}

class HITSSuite extends SparkFunSuite with LocalSparkContext {
  
  def compareScores(a: VertexRDD[(Double, Double)], b: VertexRDD[(Double, Double)]): Double = {
    a.innerZipJoin(b) { 
      case (vid, a, b) => (a._1 - b._1) * (a._1 - b._1) + (a._2 - b._2) * (a._2 - b._2) 
    }.map { case (id, error) => error }.sum()
  }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 101
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val errorTol = 1.0e-5

      val staticScores1 = starGraph.staticHITS(numIter = 1).vertices
      val staticScores2 = starGraph.staticHITS(numIter = 2).vertices

      // On a star graph, HITS should converge after one iteration
      val notMatching = staticScores1.innerZipJoin(staticScores2) { (vid, pr1, pr2) =>
        if (pr1 != pr2) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)

      // On a star graph, center vertex should have a (hub, auth) score of (0.0, 1.0)
      // while all other vertices should have (1.0 / sqrt(nVertices - 1.0), 0.0)
      val referenceScores = VertexRDD( sc.parallelize(StarHITS(nVertices)) )
      assert(compareScores(staticScores2, referenceScores) < errorTol)
    }
  } // end of test Star HITS 

  test("Chain HITS") {
    withSpark { sc =>
      val nVertices = 10
      val chain1 = (0 until (nVertices - 1)).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val numIter = 10
      val errorTol = 1.0e-5

      // On a chain graph, the first vertex should have an auth score of 0.0 and the last 
      // vertex should have a hub score of 0.0. All other hub/auth scores should be 
      // 1.0 / sqrt(nVertices - 1.0)
      val staticScores = chain.staticHITS(numIter).vertices
      val referenceScores = VertexRDD( sc.parallelize(ChainHITS(nVertices)) )
      assert(compareScores(staticScores, referenceScores) < errorTol)
    }
  } // end of test Chain HITS

  test("Grid HITS") {
    withSpark { sc =>
      val nRows = 100
      val nCols = 120
      val numIter = 10
      val errorTol = 1.0e-5

      // Generate grid graph, compute HITS, and produce reference hub and authority scores 
      val gridGraph = GraphGenerators.gridGraph(sc, nRows, nCols)
      val staticScores = gridGraph.staticHITS(numIter).vertices
      val referenceScores = VertexRDD( sc.parallelize(GridHITS(nRows, nCols, numIter)) )

      // Check to make sure that staticHITS returns similar values to our reference
      assert(compareScores(staticScores, referenceScores) < errorTol)
    }
  } // end of test Grid HITS

  test("Single Vertex HITS") {
    withSpark { sc =>
      // A graph with one vertex and no edges should have a (hub, auth) score of (0.0, 0.0)
      // and converges after one iteration
      val singleVert = Graph(sc.parallelize(Array((0L, Unit))), new EmptyRDD[Edge[Unit]](sc))
      val staticScores = singleVert.staticHITS(1).vertices
      val notMatching = if (staticScores.first()._2 != (0.0, 0.0)) 1 else 0
      assert(notMatching === 0)
    }
  } // end of test Single Vertex HITS
}
