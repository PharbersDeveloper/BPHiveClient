//import java.sql.{Connection, DriverManager, ResultSet, Statement}
//
//import org.json.JSONObject
//import org.scalatest.FunSuite
//
//class hivetest extends FunSuite{
//	test("hive") {
//		/**
//		  * 在这里实现连接 hiveserver2，访问hive
//		  */
//		val driverName: String = "org.apache.hive.jdbc.HiveDriver"
//		try{
//			Class.forName(driverName)
//		} catch {
//			case e: ClassNotFoundException =>
//				println("Missing class",e)
//		}
//
//		//创建连接
//		//		val con: Connection = DriverManager.getConnection("jdbc:hive2://spark.master:10000/default","root","password")
//		val con: Connection = DriverManager.getConnection("jdbc:hive2://192.168.100.137:10000/default", "hive", "")
//		val stmt:Statement = con.createStatement()
//		//查询
//		val result:ResultSet = stmt.executeQuery("select * from parquet_test limit 30")
//		println("------"+result)
//		val metaData = result.getMetaData
//		val titleList = Range(1, metaData.getColumnCount + 1).map(x => metaData.getColumnLabel(x).split("\\.").last).toList
//		val resultJson = new JSONObject()
//		//获取结果
//		while (result.next()) {
//			val row = Range(1, metaData.getColumnCount + 1).map(x => {
//				result.getString(x)
//			}).toList
//			val josonObject = new JSONObject()
//			row.zipWithIndex.foreach(x => josonObject.put(titleList(x._2), x._1))
//			println(josonObject)
//		}
//
//		result.close()
//		stmt.close()
//		con.close()
//	}
//}
//
