package qoePredictor

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils








object MMain {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("qoePredictor").setMaster("local")
    val sc = new SparkContext(conf)
      
    
  }
    
}