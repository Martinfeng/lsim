package lsim

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, SparseMatrix}
import org.apache.spark.rdd.RDD

import scala.math.pow

class online {
  def SinglePairSim(P: Matrix, D: DenseMatrix, i: Int, T: Int, c: Double): Array[Double] = {
    val nodes_num = P.numCols
    var gamma = new Array[Double](nodes_num)
    var x = new SparseMatrix(nodes_num, 1, Array(i - 1, i), Array(i - 1), Array(1.0)).toDense
    //var y=new SparseMatrix(nodes_num,1,Array(j-1,j),Array(j-1),Array(1.0)).toDense

    for (t <- 1 to T) {
      gamma = gamma.zip(that = (P.transpose.multiply(y = D).multiply(x) multiply new DenseMatrix(1, 1, Array(pow(c, t)))).toArray)
        .map({ case (a, b) => a + b })
      x = P.multiply(x)
      //y=P.multiply(y)
    }
    gamma map (x => if (x < 0.5) 0 else x)
  }

  def graphStruct(indexedNode: RDD[(String, Long)], nodes: RDD[(String, String)]): Graph[String, Int] = {

    val indexedNodes = nodes.join(indexedNode).map(r => (r._2._1, r._2._2)).join(indexedNode)
      .map(r => (r._2._1, r._2._2))

    val relationShips: RDD[Edge[Int]] = indexedNodes.map { x =>
      val x1 = x._1
      val x2 = x._2
      Edge(x1, x2, 1)
    }

    val users: RDD[(VertexId, String)] = indexedNode.map { x =>
      (x._2, x._1)
    }

    val graph = Graph(users, relationShips)
    graph
  }
}
