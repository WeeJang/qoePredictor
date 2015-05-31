package qoePredictor

import org.apache.spark.SparkContext

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

object RFPredictor extends Predictor {

  override def getPredErr = Err
  
  private var Err = 0d

  def run(sc: SparkContext, libSVMDataFileUri: String,splitRadio:Array[Double] = Array(0.7d,0.3d)): Unit = {

    //load and parse thd data file
    val data = MLUtils.loadLibSVMFile(sc, libSVMDataFileUri)

    //split the data into training and test set
    val splits = data.randomSplit(splitRadio)
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest Model
    // Empty categoricalFeatureInfo indicates all features are continuous
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 50
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    //Evaluate model on test instance and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    Err = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count

    //println("Learning classification forest model : \n" + model.toDebugString)

    //save and load model
    //model.save(sc, "/home/jangwee/log/model")
  }
  
  
 
 
}