package com.pharbers.hive.client.libs.spark.session

private[spark] object SparkConnConfig {
	//    val configPath: String = "pharbers_config/spark-config.xml"

	val yarnJars: String = "hdfs://spark.master:8020/jars/sparkJars"
	val yarnResourceHostname: String = "spark.master"
	val yarnResourceAddress: String = "spark.master:8032"
	val yarnDistFiles: String = "hdfs://spark.master:8020/config"
	val executorMemory: String = "2g"
}
