package qoePredictor

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors

import scala.math._

object MMain {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("qoePredictor").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("/home/jangwee/log/output.log")

    //去除不需要的数据
    val lineArrs = textFile.map { line =>
      val lineArr = line.split(" ") //分割行
      //timeStamp, RSSI,     linkSpeed,   RTT,     playbackTS,BitLayer,  layerIndex,bufferTime
      (lineArr(0), lineArr(1), lineArr(2), lineArr(4), lineArr(6), lineArr(7), lineArr(8), lineArr(9))
    }

    lineArrs.cache()

    //标识一条记录
    var isRecordFinish = false
    //样本窗口长度（second）
    val trainWindowLength = 8
    //预测窗口长度（second）
    val predictWindowLength = 4
    //遍历长度
    val travelLength = trainWindowLength + predictWindowLength
    //长度记录
    var lengthCounter = 0
    //样本记录,环形Array
    import scala.collection.mutable.ArrayBuffer
    val sampleRSSIBuffer = new ArrayBuffer[Double](travelLength)
    val sampleLinkSpeedBuffer = new ArrayBuffer[Double](travelLength)
    val sampleRTTBuffer = new ArrayBuffer[Double](travelLength)
    val sampleBitLayer = new ArrayBuffer[Double](travelLength)
    val sampleIndex = new ArrayBuffer[Double](travelLength)
    val sampleBufferTime = new ArrayBuffer[Double](travelLength)
    //cache
    val sampleCache = Array(sampleRSSIBuffer, sampleLinkSpeedBuffer, sampleRTTBuffer, sampleBitLayer, sampleIndex, sampleBufferTime)
    //Pointer
    var headPosition = 0;
    var isFull = false

    val retRDD = lineArrs.map { lineTuple =>

      val playbackTimeStamp = lineTuple._5.asInstanceOf[Double]
      //判断是否是一条新记录
      if (playbackTimeStamp < 0.499) {
        headPosition = 0
        isFull = false
      }

      sampleRSSIBuffer(headPosition) = lineTuple._2.asInstanceOf[Double]
      sampleLinkSpeedBuffer(headPosition) = lineTuple._3.asInstanceOf[Double]
      sampleRTTBuffer(headPosition) = lineTuple._4.asInstanceOf[Double]
      sampleBitLayer(headPosition) = lineTuple._6.asInstanceOf[Double]
      sampleIndex(headPosition) = lineTuple._7.asInstanceOf[Double]
      sampleBufferTime(headPosition) = lineTuple._8.asInstanceOf[Double]

      headPosition += 1
      if (headPosition == travelLength) {
        //ringBuffer is full
        isFull = true
        headPosition = 0
      }

      if (isFull) {
        //Get Feature Data
        val featureData = for {
          i <- 0 to trainWindowLength - 1
        } yield {
          val elemIndex = (i + headPosition) % travelLength
          (sampleRSSIBuffer(elemIndex),
            sampleLinkSpeedBuffer(elemIndex),
            sampleRTTBuffer(elemIndex),
            sampleBitLayer(elemIndex),
            sampleIndex(elemIndex),
            sampleBufferTime(elemIndex))
        }
        
        val formatFeatureData = featureData.foldLeft(Array(ArrayBuffer[Double](),ArrayBuffer[Double](),ArrayBuffer[Double](),
                                                                        ArrayBuffer[Double](),ArrayBuffer[Double](),ArrayBuffer[Double]())){
          (accum,elem) =>{
            accum(0).append(elem._1)  //RSSI
            accum(1).append(elem._2)  //LinkSpeed
            accum(2).append(elem._3)  //RTT
            accum(3).append(elem._4)  //BitLayer
            accum(4).append(elem._5)  //Index
            accum(5).append(elem._6)  //BufferTime
            accum
          }
        }
        
         
        //Get Result Data
        val resultData = for{
          i <- trainWindowLength to travelLength - 1
        } yield {
          val elemIndex = (i + headPosition) % travelLength
          sampleBufferTime(elemIndex)
        }
        //卡顿标记
        val freeze = if(resultData.exists(_ < 0.5)) 1 else 0
        //平均缓冲水平
        val meanBufferTime = resultData.sum / resultData.size 
        
        val finalData = ArrayBuffer[Double]()
        
        formatFeatureData.foreach{ arr =>
          finalData.append(arr : _*)
        }
        
        Some(finalData.append(freeze))
        
      } else {
        //ringBuffer is not full
        None
      }

    }
  }
}