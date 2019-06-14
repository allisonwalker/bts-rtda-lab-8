import models.{GTopicModel, MemberName, VenueNameAndLocation}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()

    val kafkaHost: String = args(0)
    val elasticHost: String = args(1)
    val operation: String =args(2)

    val meetupStreamProcessor = new MeetupStreamProcessing(spark)

    val meetupStreamStringDataset = meetupStreamProcessor.connectToKafkaStreamAndGetStringDatasetFromValue(kafkaHost)
    val meetupStreamMeetupDataset = meetupStreamProcessor.transformFromStringDatasetToMeetupmodelDataset(meetupStreamStringDataset)

    if(operation.equals("meetup-topics")) {
      val meetupTopics: Dataset[GTopicModel] = meetupStreamProcessor.extractMeetupTopics(meetupStreamMeetupDataset)


      meetupTopics.writeStream
        .format("org.elasticsearch.spark.sql")
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", "/tmp")
        .option("es.resource", "topics/es_spark")
        .option("es.nodes", elasticHost + ":9200")
        .option("es.spark.sql.streaming.sink.log.enabled", "false").start().awaitTermination()
    }

    if(operation.equals("venues-name-location")) {
      val meetupVenuesNamesLocation: Dataset[VenueNameAndLocation] = meetupStreamProcessor.extractVenueNameAndLocation(meetupStreamMeetupDataset)

      meetupVenuesNamesLocation.writeStream
        .format("org.elasticsearch.spark.sql")
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", "/tmp")
        .option("es.resource", "venuesnamelocation/venuesnamelocation")
        .option("es.nodes", elasticHost + ":9200")
        .option("es.spark.sql.streaming.sink.log.enabled", "false").start().awaitTermination()
    }

    if(operation.equals("members-name")) {
      val meetupVenuesNamesLocation: Dataset[MemberName] = meetupStreamProcessor.extractMemberName(meetupStreamMeetupDataset)

      meetupVenuesNamesLocation.writeStream
        .format("org.elasticsearch.spark.sql")
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", "/tmp")
        .option("es.resource", "membernames/membernames")
        .option("es.nodes", elasticHost + ":9200")
        .option("es.spark.sql.streaming.sink.log.enabled", "false").start().awaitTermination()
    }


    spark.stop()
  }

}
