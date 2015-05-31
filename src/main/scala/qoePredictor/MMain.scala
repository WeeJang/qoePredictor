package qoePredictor

import java.io.File

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object MMain {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("qoePredictor").setMaster("local")
    val sc = new SparkContext(conf)

    val sourceFileUri = "/home/jangwee/log/output.log"
    val libSVMDataFileUri = "/home/jangwee/log/libSVMData.txt"
    val modelUri = "/home/jangwee/log/model"

    //check model dictionary
    val modelDir = new File(modelUri)
    if (modelDir.exists) {
      Utils.removeAll(modelDir)
    } else {
    }

    //check libSVMData is exist
    if(!new File(libSVMDataFileUri).exists){
      Utils.transFile2LibSVMDataFile(sc, sourceFileUri, libSVMDataFileUri,5,5,5)
    }else{
      
    }
    
    
    

   
  }

}