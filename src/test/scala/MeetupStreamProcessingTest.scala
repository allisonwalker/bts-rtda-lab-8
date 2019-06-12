import java.sql.{Date, Timestamp}

import com.holdenkarau.spark.testing.{DatasetSuiteBase, StreamingSuiteBase}
import models.MeetupModel
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{OutputMode}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

class MeetupStreamProcessingTest  extends FunSuite with DatasetSuiteBase {
  test("Initial load json array"){
    import spark.implicits._
    val meetupModelSchema = Encoders.product[MeetupModel].schema

    val defaultValue = spark.udf.register("defaultValue",()=> new Timestamp(new DateTime().getMillis))

    val jsonDS = spark
      .readStream
      .option("multiLine", "true").schema(meetupModelSchema)
      .json("data/").as[MeetupModel]

    val venueCount = jsonDS
      .withColumn("etime", defaultValue())
      .withWatermark("etime", "4 seconds")
      .groupBy(
        window($"etime", "4 seconds", "1 seconds"),
        $"venue"
      ).agg(count($"venue").as("count"))

    venueCount.writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Append())
      .start()
      .processAllAvailable();

     assert(spark.sql("select * from Output").collectAsList.size() == 1)
  }

  test("Test read from meetup json test data"){
    import spark.implicits._
    val meetupModelSchema = Encoders.product[MeetupModel].schema

    val jsonDS = spark.readStream.option("multiLine", "true").schema(meetupModelSchema).json("data/").as[MeetupModel]

    val query = jsonDS.flatMap(meetup=>meetup.group.group_topics).groupByKey(t=>t.topic_name).count()


    query.writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Complete())
      .start()
      .processAllAvailable();

    assert(spark.sql("select * from Output").collectAsList.size() == 11)
  }

}
