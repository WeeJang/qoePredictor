package qoePredictor

import java.io.File
import org.apache.spark.SparkContext

object Utils {

  //调试
  def debug(f: => Unit)(debugFlag: String = "default"): Unit = {
    println("=====================")
    f
    println("=====================")
  }

  //删除该文件夹及文件夹下所有文件
  def removeAll(file: File): Unit = {
    file.isDirectory match {
      case true => { file.listFiles.map(removeAll _); file.delete }
      case false => file.delete()
    }
  }

  //  将数据文件转换成LibSVMData格式
  //
  //  窗口
  //  | ---Train --- | --- Interval --- | --- Predict --- | 
  //    训练窗口长度（second）
  //       trainWindowLength
  //    间隔窗口长度（second）
  //       intervalWindowLength 
  //    预测窗口长度（second）
  //       predictWindowLength  
  def transFile2LibSVMDataFile(sc: SparkContext, sourceFileUri: String, destFileUri: String,
                   trainWindowLength: Int, intervalWindowLength: Int, predictWindowLength: Int): Unit = {

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

    val travelLength = trainWindowLength + intervalWindowLength + predictWindowLength

    //样本记录,环形Array
    import scala.collection.mutable.ArrayBuffer
    val sampleRSSIBuffer = new Array[Double](travelLength)
    val sampleLinkSpeedBuffer = new Array[Double](travelLength)
    val sampleRTTBuffer = new Array[Double](travelLength)
    val sampleBitLayer = new Array[Double](travelLength)
    val sampleIndex = new Array[Double](travelLength)
    val sampleBufferTime = new Array[Double](travelLength)

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
              //             accum(3).append(elem._4) //BitLayer
              //             accum(4).append(elem._5) //Index
              accum(5).append(elem._6) //BufferTime
              accum
            }
        }

        //Get Result Data
        val resultData = for {
          i <- (trainWindowLength + intervalWindowLength) to travelLength - 1
        } yield {
          val elemIndex = (i + headPosition) % travelLength
          sampleBufferTime(elemIndex)
        }
        //卡顿标记(1:yes,-1:no)
        val freeze = if (resultData.exists(_ < 0.5)) 1 else 0
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
        //截取［RSSI,LinkSpeed,RTT］*trainWindowLength + recent_bufferlength
        val outputLine = buffered.takeRight(buffered.size - 1).take(4 * trainWindowLength).foldLeft(new StringBuffer(s"${buffered(0)} ")) { (accum, elem) =>
          index += 1
          accum.append(s"${index}:${elem} ")
        }.append(s"\n")

        bufferedWriter.write(outputLine.toString)
      }
    }
    bufferedWriter.close

  }

}