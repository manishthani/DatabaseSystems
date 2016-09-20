package relation
package compiler

import scala.collection.mutable

import ch.epfl.data.sc.pardis
import pardis.optimization.RecursiveRuleBasedTransformer
import pardis.quasi.TypeParameters._
import pardis.types._
import PardisTypeImplicits._
import pardis.ir._

import relation.deep.RelationDSLOpsPackaged
import relation.shallow._  

class ColumnStoreLowering(override val IR: RelationDSLOpsPackaged, override val schemaAnalysis: SchemaAnalysis) extends RelationLowering(IR, schemaAnalysis) {
  import IR.Predef._
  type LoweredRelation = Rep[Array[Array[String]]] 
  
  def relationScan(scanner: Rep[RelationScanner], schema: Schema, size: Rep[Int], resultSchema: Schema): LoweredRelation = {

    val schemaSize = schema.size;

    var arr : Rep[Array[Array[String]]] = dsl"""
      var array = new Array[Array[String]]($schemaSize)
      for( i <- 0 until $schemaSize) array(i) = new Array[String]($size)
      array
    """

    dsl"""
      var rowIndex = 0
      while($scanner.hasNext) {
        for (j <- 0 until $schemaSize){
          $arr(j)(rowIndex) = $scanner.next_string();
        }
        rowIndex = rowIndex + 1
      }
    """
    arr
    
  }
    

  def relationProject(relation: Rep[Relation], schema: Schema, resultSchema: Schema): LoweredRelation = {
    
    val arr = getRelationLowered(relation)
    val mySchema = getRelationSchema(relation)
    val columnIndexes = resultSchema.columns.map(column => mySchema.indexOf(column))
    var resultSchemaSize = resultSchema.size;


    def copyRecord: Rep[Array[String]] => Rep[Array[String]] = record => dsl"""
        var array = new Array[String]($arr(0).length)
        array = $record
        array
    """
   
    var resultArray = dsl"""
      var array = new Array[Array[String]]($resultSchemaSize)
      for( i <- 0 until $resultSchemaSize) array(i) = new Array[String]($arr(0).length)
      array
    """
    
    for(j <- 0 until resultSchemaSize) {
      var columnValue = columnIndexes(j)
      dsl""" 
        $resultArray($j) = $copyRecord($arr($columnValue))
      """
    }
    resultArray
  }
  
  def relationSelect(relation: Rep[Relation], field: String, value: Rep[String], resultSchema: Schema): LoweredRelation = {
    val arr = getRelationLowered(relation)
    val fieldIndex = resultSchema.indexOf(field) 
    var resultSchemaSize = resultSchema.size;

    dsl"""
      var size = 0
      val e = $arr($fieldIndex)
      for (j <- 0 until $arr(0).length) {
        if(e(j) == $value) size = size + 1
      }

      val array = new Array[Array[String]]($resultSchemaSize)

      for (i <- 0 until $resultSchemaSize) array(i) = new Array[String](size)
      
      var count = 0
      for (i <- 0 until $arr(0).length) {
        if(e(i) == $value) {
          for(k <- 0 until $resultSchemaSize){
            array(k)(count) = $arr(k)(i)
          }         
          count = count + 1
        }
      }
      array
    """
  }
  
  def relationJoin(leftRelation: Rep[Relation], rightRelation: Rep[Relation], leftKey: String, rightKey: String, resultSchema: Schema): LoweredRelation = {

    val arr1 = getRelationLowered(leftRelation)
    val arr2 = getRelationLowered(rightRelation)
    val sch1 = getRelationSchema(leftRelation)
    val sch2 = getRelationSchema(rightRelation)
    
    val sch1List = sch1.columns
    val sch2List = sch2.columns.filter(_ != rightKey)

    val sch1ListSize = sch1List.size;
    val sch2ListSize = sch2List.size;

    val indexLeftKey = sch1.indexOf(leftKey)
    val indexRightKey = sch2.indexOf(rightKey)

    val indexesLeftRel = sch1List.map(column => sch1.indexOf(column))
    val indexesRightRel = sch2List.map(column => sch2.indexOf(column))


    def getJoinRelationSize(): Rep[Int] =dsl"""
      val leftColumnJoin = $arr1($indexLeftKey)
      val rightColumnJoin = $arr2($indexRightKey)
      var size = 0
      for (i <- 0 until leftColumnJoin.length; j <- 0 until rightColumnJoin.length) {
        if(leftColumnJoin(i) == rightColumnJoin(j)) size = size + 1
      } 
      size
    """

    def joinRecords = (e1: Rep[Int], e2: Rep[Int]) => {

          val joinArray = dsl"new Array[String]($sch1ListSize+$sch2ListSize)"
          var pos = 0
          for(i <- 0 until sch1ListSize){
            var value = indexesLeftRel(i)
            dsl"$joinArray($pos) = $arr1($value)($e1)"
            pos = pos + 1
          }

          for(i <- 0 until sch2ListSize){
            var value = indexesRightRel(i)
            dsl"$joinArray($pos) = $arr2($value)($e2)"
            pos = pos + 1
          }
          joinArray
    }

    var size = getJoinRelationSize()

    var arr = dsl"""
      val array = new Array[Array[String]]($sch1ListSize+$sch2ListSize)
      for(i <- 0 until array.length) array(i) = new Array[String]($size) 
      array
    """

    dsl"""
      val leftColumnJoin = $arr1($indexLeftKey)
      val rightColumnJoin = $arr2($indexRightKey)
      var pos: Int = 0
      for (i <- 0 until leftColumnJoin.length; j <- 0 until rightColumnJoin.length) {
        if (leftColumnJoin(i) == rightColumnJoin(j)) {
          var joinRecord = $joinRecords(i,j)
          for(k <- 0 until $arr.length){
            $arr(k)(pos) = joinRecord(k)
          }
          pos = pos + 1
        }
      }
      $arr
    """
    arr
  }
  
  def relationPrint(relation: Rep[Relation]): Unit = {
    val arr = getRelationLowered(relation)
    val schema = getRelationSchema(relation)
    val schemaSize = schema.size
    
    val getRecordString = (index: Rep[Int]) => {
      val e = dsl"""
        var array = new Array[String]($schemaSize)
        for (i <- 0 until $schemaSize) array(i) = $arr(i)($index)
        array
      """
      // Build a string concatenation of the record's fields, separated by "|"
      (schema.columns zip "" #:: (Stream continually "|") foldLeft dsl""" "" """) {
        case (acc, (field, sep)) =>
          var column_index = schema.indexOf(field)
          var fieldValue = dsl"$e($column_index)"
          dsl"$acc + $sep + $fieldValue" }
    }
    dsl" for (i <- 0 until $arr(0).length) println($getRecordString(i))"
  
  }
}
