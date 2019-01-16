package com.king.learn.Flink.batch.iteration

import org.apache.flink.api.scala._

/**
  * @Author: king
  * @Date: 2019-01-16
  * @Desc: TODO 
  */

object PiEstimation {
  def main(args: Array[String]) {

    val numSamples: Long = if (args.length > 0) args(0).toLong else 1000000

    val env = ExecutionEnvironment.getExecutionEnvironment

    // count how many of the samples would randomly fall into
    // the upper right quadrant of the unit circle
    val count = env.generateSequence(1, numSamples)
      .map  { sample =>
        val x = Math.random()
        val y = Math.random()
        if (x * x + y * y < 1) 1L else 0L
      }
      .reduce(_ + _)

    // ratio of samples in upper right quadrant vs total samples gives surface of upper
    // right quadrant, times 4 gives surface of whole unit circle, i.e. PI
    val pi = count
      .map ( _ * 4.0 / numSamples)

    println("We estimate Pi to be:")

    pi.print()
  }

}
