package com.readr.spark.common

abstract trait ProgressReporter {  
  def incrementCounter()
}