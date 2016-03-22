package main.scala;
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext


object Project2 {
  def filterComments(comment: String ) : Boolean =  !(comment.matches(".*special.*requests.*"))
  
  def main(args: Array[String]) {

      if(args.length != 3) {
         println("USAGE: Project2 [inputfile ] [newoutputdir] [option]");
         System.exit(1);
      }
      else {
        if(args(2) == 1){
            // Setting Spark
            val conf = new SparkConf().setAppName("Project2")
            // Setting Kryo serializer
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 
            val spark = new SparkContext(conf)
        
            // Paths to customers and orders
            val customers = "/customer.tbl"
            val orders = "/orders.tbl"
            // Load data, number of partitions high in order to avoid exceptions
            val listCustomers = spark.textFile(args(0) + customers).map(line => line.split("\\|").toList).repartition(200) 
            val listOrders = spark.textFile(args(0) + orders).map(line => line.split("\\|").toList).repartition(200)
            
            // Remove attributes we will not use for the query
            val listCustomerStruct = listCustomers.mapPartitionsWithIndex{
                (index, iterator) => {
                  val myList = iterator.toList
                  myList.map(x => (x(0).toInt -> x(0).toInt)).iterator
                }
             }
    
            //Filtering comments 
            val listOrdersGroupedComments = listOrders.filter(x => filterComments(x(8))).mapPartitionsWithIndex{
                (index, iterator) => {
                  val myList = iterator.toList
                  myList.map(x => (x(1).toInt -> x(0).toInt)).iterator
                }
             }
            
            // Performing left outer join
             val keys = spark.broadcast(listOrdersGroupedComments.map(_._1).collect.toSet)
             val reducedRDD = listCustomerStruct.filter{ case(key, value) => keys.value.contains(key) }
             val bigListCustomersJoin = reducedRDD.leftOuterJoin(listOrdersGroupedComments)
    
             // Inner select Map/Reduce
             val biglistOrdersGroupedComments = bigListCustomersJoin.mapPartitionsWithIndex{
                (index, iterator) => {
                  val myList = iterator.toList
                  myList.map(x => ( if(x._2._2 == None) (x._1)-> 0 else (x._1) -> 1 )).iterator
                }
             }.reduceByKey(_+_)
             
             // Outer select Map/Reduce
             val bigListCustomersJoinOuterSelect = biglistOrdersGroupedComments.mapPartitionsWithIndex{
                  (index, iterator) => {
                    val myList = iterator.toList
                    myList.map(x=> x._2 -> 1).iterator
                  }
             }.reduceByKey(_+_)
            
            //Map output to corresponding format
            val resultBigListFormat = bigListCustomersJoinOuterSelect.mapPartitionsWithIndex{
                  (index, iterator) => {
                    val myList = iterator.toList
                    myList.map(x => x._1 + "|" + x._2 + "|").iterator
                  }
            } 
           // Write in Output path
            var outputDir : String = "/out" + "_" + args(2).toString()
            resultBigListFormat.saveAsTextFile(args(1) + outputDir)
            spark.stop()
        }
        else{
            // Setting Spark
            val conf = new SparkConf().setAppName("Project2")
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            
            val spark = new SparkContext(conf)
            
            // Paths to orders
            val orders = "/orders.tbl"
            // Load data
            val listOrders = spark.textFile(args(0) + orders).map(line => line.split("\\|").toList).repartition(200)
            
            //Filtering comments and inner select Map/Reduce
            val listOrdersGroupedComments = listOrders.filter(x => filterComments(x(8))).mapPartitionsWithIndex{
                              (index, iterator) => {
                                 val myList = iterator.toList
                                 myList.map(x => (x(1).toInt ->1)).iterator
                              }
                           }.reduceByKey(_+_)
            
            // Outer select Map/Reduce
            val bigListCustomersJoinOuterSelect = listOrdersGroupedComments.mapPartitionsWithIndex{
                              (index, iterator) => {
                                 val myList = iterator.toList
                                 myList.map(x=> x._2 -> 1).iterator
                              }
                           }.reduceByKey(_+_)
            
            // Map output to corresponding format
            val resultBigListFormat = bigListCustomersJoinOuterSelect.mapPartitionsWithIndex{
                              (index, iterator) => {
                                 val myList = iterator.toList
                                 myList.map(x => x._1 + "|" + x._2 + "|").iterator
                              }
                           } 
           // Write in Output path
            var outputDir : String = "/out" + "_" + args(2).toString()
            resultBigListFormat.saveAsTextFile(args(1) + outputDir)
            spark.stop()
          }
      }
  }
}