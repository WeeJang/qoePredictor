package qoePredictor

import org.apache.spark.SparkContext

object Utils {

  //调试
  def debug(f: => Unit)(debugFlag: String = "default"): Unit = {
    println("=====================")
    f
    println("=====================")
  }
  
  //将数据文件转换成LibSVMData格式
  def transFile2LibSVMDataFile(sc: SparkContext, sourceFileUri: String, destFileUri: String): Unit = {

    val textFile = sc.textFile(sourceFileUri)

    import java.io._
    val file = new File(destFileUri)
    val bufferedWriter = new BufferedWriter(new FileWriter(file))

    //去除不需要的数据
    val lineArrs = textFile.map { line =>
      val lineArr = line.split("\t") //分割行
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

    //样本记录,环形Array
    import scala.collection.mutable.ArrayBuffer
    val sampleRSSIBuffer = new Array[Double](travelLength)
    val sampleLinkSpeedBuffer = new Array[Double](travelLength)
    val sampleRTTBuffer = new Array[Double](travelLength)
    val sampleBitLayer = new Array[Double](travelLength)
    val sampleIndex = new Array[Double](travelLength)
    val sampleBufferTime = new Array[Double](travelLength)
    //cache
    val sampleCache = Array(sampleRSSIBuffer, sampleLinkSpeedBuffer, sampleRTTBuffer, sampleBitLayer, sampleIndex, sampleBufferTime)
    //Pointer
    var headPosition = 0;
    var isFull = false

    val retRDD = lineArrs.map { lineTuple =>

      val playbackTimeStamp = lineTuple._5.toDouble
      //判断是否是一条新记录
      if (playbackTimeStamp == 0.0) {
        headPosition = 0
        isFull = false
      }

      sampleRSSIBuffer(headPosition) = lineTuple._2.toDouble
      sampleLinkSpeedBuffer(headPosition) = lineTuple._3.toDouble
      sampleRTTBuffer(headPosition) = lineTuple._4.toDouble
      sampleBitLayer(headPosition) = lineTuple._6.toDouble
      sampleIndex(headPosition) = lineTuple._7.toDouble
      sampleBufferTime(headPosition) = lineTuple._8.toDouble

      headPosition += 1
      if (headPosition == travelLength) {
        //ringBuffer is full
        isFull = true
        headPosition = 0
      }

      val optionElem = if (isFull) {
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

        val formatFeatureData = featureData.foldLeft(Array(ArrayBuffer[Double](), ArrayBuffer[Double](), ArrayBuffer[Double](),
          ArrayBuffer[Double](), ArrayBuffer[Double](), ArrayBuffer[Double]())) {
          (accum, elem) =>
            {
              accum(0).append(elem._1) //RSSI
              accum(1).append(elem._2) //LinkSpeed
              accum(2).append(elem._3) //RTT
              accum(3).append(elem._4) //BitLayer
              accum(4).append(elem._5) //Index
              accum(5).append(elem._6) //BufferTime
              accum
            }
        }

        //Get Result Data
        val resultData = for {
          i <- trainWindowLength to travelLength - 1
        } yield {
          val elemIndex = (i + headPosition) % travelLength
          sampleBufferTime(elemIndex)
        }
        //卡顿标记(1:yes,-1:no)
        val freeze = if (resultData.exists(_ < 0.5)) 1 else -1
        //平均缓冲水平
        val meanBufferTime = resultData.sum / resultData.size

        val finalData = ArrayBuffer[Double]()

        finalData.append(freeze)

        formatFeatureData.foreach { arr =>
          finalData.append(arr: _*)
        }

        Some(finalData)
      } else {
        //ringBuffer is not full
        None
      }
      optionElem
    }

    retRDD.collect().map { arrBuffer =>
      arrBuffer.foreach { buffered =>
        var index = 0
        val outputLine = buffered.takeRight(buffered.size - 1).foldLeft(new StringBuffer(s"${buffered(0)} ")) { (accum, elem) =>
          index += 1
          accum.append(s"${index}:${elem} ")
        }
        outputLine.append("\n")
        bufferedWriter.write(outputLine.toString)
      }
    }
    bufferedWriter.close

  }

}