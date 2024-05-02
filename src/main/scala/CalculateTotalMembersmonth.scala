import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object CalculateTotalMembersmonth {

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("CalculateTotalMembers")
      .getOrCreate()

    try {
      // Load member_eligibilty.csv csv file into spark DataFrame
      val memberEligibility = spark.read.option("header", "true").csv("member_eligibilty.csv").cache()

      // Load member_months.csv
      val memberMonths = spark.read.option("header", "true").csv("member_months.csv").cache()

      // Join eligibility data with member data
      val joinedData = memberEligibility.join(memberMonths, Seq("member_id"))

      /*
        1. Given the two data datasets, calculate the total number of members months.
        The resulting set should contain the member's ID, full name, along with the number of member months.
        Output the report in json, partitioned by the membersID.
       */
      val memberMonths = joinedData.withColumn("year", year($"eligibility_effective_date")).withColumn("month", month($"eligibility_effective_date")).
        .groupBy($"member_id", $"first_name", $"middle_name", $"last_name", $"year", "$month")
        .agg(sum($"eligibility_member_month").alias("total_member_months"))

      result_1 = memberMonths.select($"member_id", $"first_name", $"middle_name", $"last_name")

      // Convert DataFrame to JSON and write to file partitioned by member ID
      result_1.write.partitionBy("member_id").json("Usecaes1_JSONOutput")

      /* 2.Given the two datasets, calculate the total number of member months per member per year.
         The resulting set should contain the member's ID, the month, and total number of member months. Output the result set in json.
       */

      val memberMonthsPerYear = memberMonths.
        .groupBy($"member_id", $"year", $"month")
        .agg(sum($"eligibility_member_month").alias("total_member_months"))

      result_2 = memberMonthsPerYear.select($"member_id", $"month")

      // Convert DataFrame to JSON and write to file
      result_2.write.partitionBy("member_id", "year", "month").json("Usecaes2_JSONOutput")
    } catch {
      case e: Exception =>
        // Log the exception
        println(s"An error occurred: ${e.getMessage}")
    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }
}
