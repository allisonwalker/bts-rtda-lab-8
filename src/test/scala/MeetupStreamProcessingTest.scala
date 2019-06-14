import java.sql.{Date, Timestamp}

import com.holdenkarau.spark.testing.{DatasetSuiteBase, StreamingSuiteBase}
import models.{GTopicModel, MeetupModel}
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

class MeetupStreamProcessingTest  extends FunSuite with DatasetSuiteBase {

  def getTestDataStream(): Dataset[MeetupModel] ={
    import spark.implicits._
    val meetupModelSchema = Encoders.product[MeetupModel].schema

    spark.readStream
      .option("multiLine", "true")
      .schema(meetupModelSchema)
      .json("data/").as[MeetupModel]
  }

  test("Test extract topics"){
    //read test data
    val meetupStreamDataset = getTestDataStream();

    //Init meetup processor object
    val meetupStreamProcessor = new MeetupStreamProcessing(spark);

    // process meetup dataset
    val result = meetupStreamProcessor.extractMeetupTopics(meetupStreamDataset)

    //Write result to a temporal table "Output" in memory
    result.writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Append())
      .start()
      .processAllAvailable();

    //Check Output
    val realOutput: Array[Row] = spark.sql("select * from Output").collect()

    assert(realOutput.length == 21)
  }

  test("Test extract venues names and location"){
    //read test data
    val meetupStreamDataset = getTestDataStream();

    //Init meetup processor object
    val meetupStreamProcessor = new MeetupStreamProcessing(spark);

    // process meetup dataset
    val result = meetupStreamProcessor.extractVenueNameAndLocation(meetupStreamDataset)

    //Write result to a temporal table "Output" in memory
    result.writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Append())
      .start()
      .processAllAvailable();

    //Check Output
    val realOutput: Array[Row] = spark.sql("select * from Output").collect()

    assert(realOutput.length == 2)
  }

}
