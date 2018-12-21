package ru.kai.practice.practice5

import org.apache.spark.mllib.feature.HashingTF

/**
  * @author alexander.leontyev
  */
object TrainUtil {

  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

}