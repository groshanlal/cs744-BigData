import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object PartBApplication2Question2 {
  def main(args: Array[String]) {
    
    val spark = SparkSession
        .builder
        .master("spark://10.254.0.212:7077")
        .appName("PartBApplication2Question2")
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
    
    val popular_vertex: VertexRDD[(Int, Array[String])] = graph.aggregateMessages[(Int, Array[String])](
      triplet => {
        triplet.sendToDst(1, triplet.dstAttr)
      },
      (a,b) => (a._1 + b._1, a._2)
    )
    val most_popular_vertex = popular_vertex.reduce(max)
    println("The id of the most popular vertex is: " + most_popular_vertex._1)
    //most_popular_vertex._2._2.foreach(println(_))
  }

  def checkCommonString(a: Array[String], b: Array[String]): Boolean =  {
    val aSet = a.toSet
    val bSet = b.toSet
    val unionSet = aSet & bSet
    return if (unionSet.size == 0) false else true
  }

  def max(a: (VertexId,(Int,Array[String])), b: (VertexId, (Int,Array[String]))): (VertexId, (Int,Array[String])) = {
    if (a._2._1 == b._2._1)
      return if (a._2._2.length > b._2._2.length) a else b
    return if (a._2._1 > b._2._1) a else b
  }
}