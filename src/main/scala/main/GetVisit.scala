package main

/**
 * @author Zhuotao Zhang
 * @date 18/Jun/2015
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import functions.VisitCalculator

/**
 * THis class is an app for generating visits
 */
object GetVisit extends App {
  
  var conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  VisitCalculator.register(conf, "hdfs://localhost:8020", "hdfs://localhost:8020/user/lewis/log_data/pages_fixed_test", "/user/lewis", 2)
  VisitCalculator.calculate()
  
}