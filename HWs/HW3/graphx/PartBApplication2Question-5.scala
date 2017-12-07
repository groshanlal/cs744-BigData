import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object PartBApplication2Question5 {
  def main(args: Array[String]) {
    
    val spark = SparkSession
        .builder
        .master("spark://10.254.0.212:7077")
        .appName("PartBApplication2Question5")
        .config("spark.driver.memory", "1g")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/home/ubuntu/logs/apps/")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "4")
        .config("spark.task.cpus", "1")
        .getOrCreate()
    val sc = spark.sparkContext
  
    val vfile = sc.textFile("vertices.txt")
    val vRDD: RDD[(VertexId, Array[String])] = vfile.map(line => line.split(","))
            .zipWithIndex()
            .map(_.swap)

    val eRDD:RDD[Edge[(VertexId, VertexId)]] = vRDD.cartesian(vRDD)
                  .filter(x => (x._1._1 != x._2._1) && checkCommonString(x._1._2, x._2._2) == true)
                  .map(x => Edge(x._1._1, x._2._1))

    val graph = Graph(vRDD, eRDD)
  
    val cc = graph.connectedComponents

    /*graph.vertices.leftJoin(cc.vertices) {
      case (id, attr, comp) => s"${id} is in component ${comp.get}"
    }.collect.foreach{ case (id, str) => println(str) }*/

    //cc.vertices.collect.foreach(println(_))

    val cc_size:RDD[(Long,Int)] = cc.vertices.map( a => (a._2, 1) )
                .reduceByKey(_+_)
                
    val max_size_cc = cc_size.reduce(max_cc)
    //cc_size.collect.foreach(println(_))
    println("The largest subgraph has size: " + max_size_cc._2)
  }
  
  def checkCommonString(a: Array[String], b: Array[String]): Boolean =  {
    val aSet = a.toSet
    val bSet = b.toSet
    val unionSet = aSet & bSet
    return if (unionSet.size == 0) false else true
  }

  def max(a: (String, Int), b: (String, Int)): (String, Int) = {
    return if (a._2 > b._2) a else b
  }

  def max_cc(a: (Long, Int), b: (Long, Int)): (Long, Int) = {
    return if (a._2 > b._2) a else b
  }
}