import com.pharbers.hive.client.libs.spark.PhSparkDriver
import org.scalatest.FunSuite

class sparkOnHiveTest extends FunSuite{
	test("spark sql on hive") {
		implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
		val sparkSession = sd.ss
		sparkSession.sparkContext.addJar("hdfs://spark.master:8020/jars/hiveJars")
		import sparkSession.implicits._
		import sparkSession.sql
		sql("use phmax")
		val dfc = sql("CREATE TABLE phhosp2 " +
			"(PHA_ID string, HOSP_NAME string, HOSP_LEVEL string, HOSP_CHARACTER string, DOCTOR_NUM int, " +
			"ACTIVE_EMPLOYEE_NUM int, EST_DRUGINCOME_RMB double, ANNUAL_VISIT_NUM int, INTERNAL_MEDICINE_NUM int, " +
			"SURGICAL_VISIT_NUM int, OUTPATIENT_NUM int, " +
			"INPATIENT_NUM int, SURGERY_NUM int, BED_NUM int, ALL_DEPARTMENT_NUM int, INTERNAL_MEDICINE_BED_NUM int, " +
			"SURGICAL_BED_NUM int, OPHTHALMIC_BED_NUM int," +
			"MEDICAL_INCOME double, OUTPATIENT_INCOME double, OUTPATIENT_TREATMENT_INCOME double, " +
			"OUTPATIENT_SURGERY_INCOME double, INPATIENT_INCOME double, INPATIENT_BED_INCOME double, " +
			"INPATIENT_TREATMENT_INCOME double, INPATIENT_SURGERY_INCOME double, DRUG_INCOME double, " +
			"OUTPATIENT_MEDICINE_INCOME double, OUTPATIENT_WESTERN_MEDICINE_INCOME double, " +
			"INPATIENT_MEDICINE_INCOME double, INPATIENT_WESTERN_MEDICINE_INCOME double, SPECIALTY_1 string, " +
			"RE_SPEIALTY string, SPECIALTY_3 string, SPECIALTY_1_STANDARDIZATION string, SPECIALTY_2 string, " +
			"SPECIALTY_2_STANDARDIZATION string, REGION string, LOCATION string, PROVINCE string, CITY string, " +
			"PREFECTURE string, ADDRESS_TYPE string, CITY_TIER_2010 string, CITY_TIER_2018 string)" +
			//			" PARTITIONED BY (ADDRESS_AREA string)" +
			" STORED AS parquet" +
			" LOCATION '/user/cui/hiveTest/HOSP5'")
		dfc.show(false)
		val selectDF = sql("select * from phhosp2")
		selectDF.show(false)
		val schemaDF = sql("desc phhosp2")
		schemaDF.show(100, false)
	}
}
