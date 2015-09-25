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
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators


object StarHITS {
  def apply(nVertices: Int): Seq[(VertexId, (Double, Double))] = {
    var hubAuth = Array.fill(nVertices){ (1.0/sqrt(nVertices - 1.0), 0.0) }
    hubAuth(0) = (0.0, 1.0)
    (0L until (nVertices)).zip(hubAuth)
  }
}

object ChainHITS {
  def apply(nVertices: Int): Seq[(VertexId, (Double, Double))] = {
    val score = 1.0/sqrt(nVertices - 1.0)
    var hubAuth = Array.fill(nVertices){ (score, score)  }
    hubAuth(0) = (score, 0.0)
    hubAuth(nVertices - 1) = (0.0, score)
    (0L until (nVertices)).zip(hubAuth)
  }
}

class HITSSuite extends SparkFunSuite with LocalSparkContext {
  
  def compareScores(a: VertexRDD[(Double, Double)], b: VertexRDD[(Double, Double)]): Double = {
    a.innerZipJoin(b) { case (vid, a, b) => (a._1-b._1)*(a._1-b._1) + (a._2-b._2)*(a._2-b._2) }
      .map{ case (id, error) => error }.sum()
  }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 101
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val errorTol = 1.0e-5

      val staticScores1 = starGraph.staticHITS(numIter = 1).vertices
      val staticScores2 = starGraph.staticHITS(numIter = 5).vertices

      // On a star graph, HITS should converge after one iteration
      val notMatching = staticScores1.innerZipJoin(staticScores2) { (vid, pr1, pr2) =>
        if (pr1 != pr2) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)

      // On a star graph, center vertex should have a (hub, auth) score of (0.0,1.0)
      // while all other vertices should have (1.0/sqrt(nVertices-1.0),0.0)
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
      // 1.0/sqrt(nVertices-1.0)
      val staticScores = chain.staticHITS(numIter).vertices
      val referenceScores = VertexRDD( sc.parallelize(ChainHITS(nVertices)) )
      assert(compareScores(staticScores, referenceScores) < errorTol)
    }
  } // end of test Chain HITS
}
