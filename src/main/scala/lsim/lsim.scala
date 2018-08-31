package lsim

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{SparseMatrix,Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry,IndexedRow,IndexedRowMatrix}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


object lsim {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("LSIM")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._
    val data = spark.sql("select " +
      "pri_acct_no_conv " +
      ",concat_ws('|',mchnt_tp,mchnt_cd,term_id,card_accptr_nm_addr) as mchnt " +
      "from sna.sh_sample " +
      "where amt_sum>1000")
      .groupBy("pri_acct_no_conv", "mchnt").count()
      .filter(col("count") > 1)
      .drop("count")
      .repartition(col("pri_acct_no_conv"), col("mchnt"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val mchnt = data.groupBy("mchnt").count().filter(col("count") > 1).drop("count").rdd.map(x => (x(0), 1))
    val card = data.groupBy("pri_acct_no_conv").count().drop("count").rdd.map(x => (x(0), 1))

    val edges = data.rdd.map(x => (x(0), x(1))).join(card).map(x => (x._2._1, x._1))
      .join(mchnt).map(x => (x._2._1.toString, x._1.toString))

    data.unpersist()

    //开始图计算
    val index2Node = (edges.map(_._1) union edges.map(_._2)).distinct.zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val nodesNum = index2Node.count().toInt

    val graph = new online().graphStruct(index2Node, edges)

    val outs = graph.outDegrees.map(x => (x._1, (1 / x._2.toDouble).formatted("%.4f").toDouble))
    val ins = graph.inDegrees.map(x => (x._1, (1 / x._2.toDouble).formatted("%.4f").toDouble))

    val rdd_out = graph.outerJoinVertices(outs)((id, _, degin) => (id.toString, degin.getOrElse(0)))
      .triplets.map { x =>
      ((x.dstId.toInt, x.srcId.toInt), x.srcAttr._2.toString.toDouble * x.attr.toInt)
    }

    val rdd_in = graph.outerJoinVertices(ins)((id, _, degin) => (id.toString, degin.getOrElse(0)))
      .triplets.map { x =>
      ((x.srcId.toInt, x.dstId.toInt), x.dstAttr._2.toString.toDouble * x.attr.toInt)
    }

    val rdd_all = rdd_out.union(rdd_in).sortByKey()

    println("*****************************rdd_all finished**********************************")

    val D = new SparseMatrix(nodesNum, nodesNum, 0 to nodesNum toArray, 0 until nodesNum toArray
      , new offline().Algo4(3, nodesNum, 3, 0.6, 10, rdd_all))

    println("*******************************D Finished*************************************")

    val transferMatrix = new CoordinateMatrix(rdd_all.map { x =>
      MatrixEntry(x._1._1, x._1._2, x._2)
    }).toBlockMatrix().toLocalMatrix()

    val simResult = spark.sparkContext.parallelize(0 until nodesNum).map(x => (x.toLong,new online()
      .SinglePairSim(transferMatrix, D.toDense, x, 10, 0.6))).map({case (x,array)=>IndexedRow(x,Vectors.dense(array))})

    val node2index=index2Node.map(x => (x._2, x._1)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val result=new IndexedRowMatrix(simResult).toCoordinateMatrix().entries
      .map({case MatrixEntry(i,j,sim)=>(i,(j,"%.4f" format sim))})
      .join(node2index)
      .map(x => (x._2._1._1, (x._2._1._2, x._2._2))).join(node2index)
      .map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
      .filter(x => !x._1.equals(x._2._1)).map(x => (x._1, x._2._1, x._2._2.toDouble))
      .toDF("item1","item2","sim")

    spark.sql("use auto_hadoop_jobs")
    result.createOrReplaceTempView("lsimresulttemp")
    spark.sql("drop table if exists lsimresult")
    spark.sql("create table lsimresult as select * from lsimresulttemp")


  }
}
