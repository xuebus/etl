import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-11-3.
 */
object TestPartition {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("etl-mongodb to hdfs").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")))
    println(rdd.partitions.size)
    rdd.mapPartitionsWithIndex{
      (partIdx,iter) => {
        var part_map = scala.collection.mutable.Map[String,List[(Int,String)]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx;
          var elem = iter.next()
          if(part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(Int,String)]{elem}
          }
        }
        part_map.iterator
      }
    }.collect
  }
}
