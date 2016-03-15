package main.scala;
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

object Project1 {
  
  def filterComments(comment: String ) : Boolean =  !(comment.matches(".*special.*requests.*"))
  
  def main(args: Array[String]) {

      if(args.length != 2) {
         println("USAGE: Project1 <inputfile> <newoutputdir>");
         System.exit(1);
      }
      // Setting Spark
      val conf = new SparkConf().setAppName("Project1")
            //conf.setMaster("yarn-cluster")
  
      val spark = new SparkContext(conf)
  
      // Paths to customers and orders
      val customers = "/customer.tbl"
      val orders = "/orders.tbl"
      
      //Reading input and parsing it
      val listCustomers = spark.textFile(args(0) + customers).map(line => line.split("\\|").toList)
      val listOrders = spark.textFile(args(0) + orders).map(line => line.split("\\|").toList)
  
      // Casting each element of lists to (key -> (values))
      val listCustomerStruct = listCustomers.map(x => (x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6), x(7)))
      val listCustomersGrouped = listCustomerStruct.map(x => (x._1.toInt -> x))
      
      val listOrdersStruct = listOrders.map(x => (x(0)toInt, x(1).toInt, x(2), x(3), x(4), x(5), x(6), x(7),x(8)))
      val listOrdersGrouped = listOrdersStruct.map(x => (x._2.toInt -> x))
       
      // Where condition of inner query (Comments filtering and LeftOuterJoin)
      val listOrdersGroupedComments = listOrdersGrouped.filter(x => filterComments(x._2._9))
      val bigListCustomersJoin = listCustomersGrouped.leftOuterJoin(listOrdersGroupedComments)
      
      
      // Inner Select to get Order_count
      val bigListCustomersJoinInnerSelect = bigListCustomersJoin.groupBy(_._1).map(x => if(x._2.size > 1) (x._1,x._2.size) else (x._1, x._2.toList(0)._2._2.size));
      

      //Outer Select to get final query result
      val bigListCustomersJoinOuterSelect = bigListCustomersJoinInnerSelect.groupBy(_._2).map(x => (x._1,x._2.size));
      
      //Parse it like the format of the input file
      val resultBigListFormat = bigListCustomersJoinOuterSelect.map(x => x._1 + "|" + x._2 + "|")
  
      // Write in Output path
      var outputDir : String = "/out"
      resultBigListFormat.saveAsTextFile(args(1) + outputDir)
  
      spark.stop()
  }
}
