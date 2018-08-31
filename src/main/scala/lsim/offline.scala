package lsim

import org.apache.spark.rdd.RDD
import scala.math.pow
import scala.util.Random.nextInt
import scala.collection.Map

class offline {
  //蒙特卡洛模拟随机游走
  def OneWalk(Walk:Map[Int,Array[Int]], i: Int): Int = {
    if(Walk.contains(i)) Walk(i)(nextInt(Walk(i).length))
    else -1
  }

  //计算矩阵D所需的两个参数
  def Algo5(k: Int, R: Int, c: Double, D: List[Double], T: Int, Walk:Map[Int,Array[Int]]): Double = {
    var alpha = 0.0
    var beta = 0.0
    var K = List.fill(R)(k)
    for (t <- 1 to T) {
      for (i <- K) {
        var pk = K.count(s => s == i).toDouble
        if (i == k) alpha = alpha + pow(c, t) * pow(pk, 2)
        beta = beta + pow(c, t) * pow(pk, 2) * D(i - 1)
      }
      K = K.map(s => OneWalk(Walk, s)).filter(_ != (-1))
    }
    (1 - alpha) / beta
  }

  //计算矩阵D
//  def Algo40(L: Int, Nodes_num: Int, R: Int, c: Double, T: Int, G: RDD[((Int, Int), Double)]): Array[Double] = {
//    var D = List.fill(Nodes_num)(1.0)
//    for (i <- 1 to L) {
//      for (k <- 1 to Nodes_num) {
//        val (alpha, beta) = Algo5(k, R, c, D, T, G)
//        var delta = (1 - alpha) / beta
//        D = D.patch(k - 1, Seq(D(k - 1) + delta), 1)
//      }
//    }
//    D.toArray
//  }
  @transient
  def Algo4(L: Int, Nodes_num: Int, R: Int, c: Double, T: Int, G: RDD[((Int, Int), Double)]): Array[Double] = {
    val Walk=G.filter(x=>x._2==1).map(x=>(x._1._1,x._1._2)).groupBy(x=>x._1)
      .map(x=>(x._1,x._2.toArray.map(y=>y._2)))
      .collectAsMap
    G.sparkContext.broadcast(Walk)
    val D_list = List.fill(Nodes_num)(1.0)
    val i = 1 to L toArray
    val k = 1 to Nodes_num toArray
    val D = G.sparkContext.parallelize(i).cartesian(G.sparkContext.parallelize(k))
      .map(x => (x._2, Algo5(x._2, R, c, D_list, T, Walk))).reduceByKey(_ + _ + 1).map(x => x._2)
      .toLocalIterator.toArray
    D
  }

}
