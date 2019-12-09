package com.pharbers.hive.client.libs.spark.session

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/** SPARK 连接实例
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
trait SparkConnInstance {

	//    System.setProperty("HADOOP_USER_NAME","spark")

	/** SPARK 连接实例名
	  *
	  * @author clock
	  * @version 0.1
	  * @since 2019/6/17 11:08
	  */
	val applicationName: String

	/** SPARK 连接配置
	  *
	  * @author clock
	  * @version 0.1
	  * @since 2019/6/17 11:08
	  */
	val connConf: SparkConnConfig.type = SparkConnConfig
	val warehouseLocation: String = new File("hdfs://spark.master:8020/user/hive/warehouse").getAbsolutePath

	private val conf = new SparkConf()
		.set("spark.yarn.jars", connConf.yarnJars)
		.set("spark.yarn.archive", connConf.yarnJars)
		.set("yarn.resourcemanager.hostname", connConf.yarnResourceHostname)
		.set("yarn.resourcemanager.address", connConf.yarnResourceAddress)
		.setAppName(applicationName)
		.setMaster("yarn")
		.set("spark.scheduler.mode", "FAIR")
		.set("spark.sql.crossJoin.enabled", "true")
		.set("spark.yarn.dist.files", connConf.yarnDistFiles)
		//		.set("spark.executor.memory", connConf.executorMemory)
		//		.set("spark.executor.memory", "512m")
		.set("spark.executor.memory", "3g")
		//		.set("spark.worker.memory", "1g")
		.set("spark.executor.cores", "2")
		//		.set("SPARK.WORKER.CORES", "1")
		.set("spark.executor.instances", "2")
		.set("spark.driver.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,adress=5005")
		.set("hive.metastore.uris", "thrift://192.168.100.137:9083")
		.set("spark.executor.extraJavaOptions",
			"""
			  | -XX:+UseG1GC -XX:+PrintFlagsFinal.set("spark.sql.warehouse.dir", warehouseLocation)
			  |
			  | -XX:+PrintReferenceGC -verbose:gc
			  | -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
			  | -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions
			  | -XX:+G1SummarizeConcMark
			  | -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=1
			""".stripMargin)

	/** SPARK Session
	  *
	  * @author clock
	  * @version 0.1
	  * @since 2019/6/17 11:08
	  */
	implicit val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

	/** SPARK Context
	  *
	  * @author clock
	  * @version 0.1
	  * @since 2019/6/17 11:09
	  */
	implicit val sc: SparkContext = ss.sparkContext

	/** SPARK SQL Context
	  *
	  * @author clock
	  * @version 0.1
	  * @since 2019/6/17 11:09
	  */
	implicit val sqc: SQLContext = ss.sqlContext
}

