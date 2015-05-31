package qoePredictor

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils



object SVMPredictor extends Predictor{

  override def getPredErr = Err
  
  private var Err = 0.0d
  
  def run(sc: SparkContext,libSVMDataFileUri:String,splitRadio: Array[Double] = Array(0.7d,0.3d)) : Unit = {
    
    //load training data in libSVM format
    val data = MLUtils.loadLibSVMFile(sc, libSVMDataFileUri)
    
    //split data into training
    
    val splits = data.randomSplit(splitRadio)
    val training =splits(0).cache()
    val test = splits(1)
    
    //Run training algorithm to build the model
    val numIterations = 100
    
    val model = SVMWithSGD.train(training, numIterations)
    
    //clear the default threadshold
    model.clearThreshold()
    
    //compute raw scores on test set
    val scoreAndLabels = test.map { point =>
       (model.predict(point.features),point.label)  
    }
    
    //Get evaluation metrics
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    
    val precision = metrics.precisionByThreshold()
     
  }
  
}