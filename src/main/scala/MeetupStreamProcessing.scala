import models.{GTopicModel, MeetupModel, MemberName, VenueNameAndLocation}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

class MeetupStreamProcessing (spark: SparkSession) {

  def connectToKafkaStreamAndGetStringDatasetFromValue(kafkaHost: String): Dataset[String] = {
    import spark.implicits._
    val kafkaStreamStringDataset = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost + ":9092")
      .option("subscribe", "stream")
      .load()
      .selectExpr("CAST(value AS STRING)").as[String]
    return kafkaStreamStringDataset
  }

  def transformFromStringDatasetToMeetupmodelDataset(kafkaStreamStringDataset: Dataset[String]): Dataset[MeetupModel] = {
    import spark.implicits._
    val dsMeetups = kafkaStreamStringDataset
      .map(r=> { implicit val formats = DefaultFormats; parse(r).extract[MeetupModel] } )
    return dsMeetups
  }

  //Processing methods
  def extractMeetupTopics(meetupStreamDataset: Dataset[MeetupModel]): Dataset[GTopicModel] = {
    import spark.implicits._
    return meetupStreamDataset
      .flatMap(meetup => meetup.group.group_topics )
      .as[GTopicModel]
  }

  //Processing methods
  def extractVenueNameAndLocation(meetupStreamDataset: Dataset[MeetupModel]): Dataset[VenueNameAndLocation] = {
    import spark.implicits._
    return meetupStreamDataset
      .map(meetup =>
        VenueNameAndLocation(
          meetup.venue.venue_name.getOrElse(""),
          meetup.venue.lat.getOrElse(0.0) + "," + meetup.venue.lon.getOrElse(0.0))
      )
      .as[VenueNameAndLocation]
  }

  def extractMemberName(meetupStreamDataset: Dataset[MeetupModel]): Dataset[MemberName] = {
    import spark.implicits._
    return meetupStreamDataset
      .map(meetup => MemberName(meetup.member.member_name.getOrElse("")))
      .as[MemberName]
  }

}
