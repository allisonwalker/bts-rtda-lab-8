package models

case class VenueModel(venue_name: Option[String], lon: Option[Double], lat: Option[Double], venue_id: Option[String])
case class MemberModel(member_id: Long, photo: Option[String], member_name: Option[String])
case class Event(event_name: String, event_id: String, time: Long, event_url: Option[String])
case class GTopicModel(urlkey: String, topic_name: String)
case class GroupModel(group_topics: Array[GTopicModel], group_city: String, group_country: String, group_id: Long, group_name: String, group_lon: Double, group_urlname: String, group_state: Option[String], group_lat: Double)
case class MeetupModel(venue: VenueModel, visibility: String, response: String, guests: Long, member: MemberModel, rsvp_id: Long,  mtime: Long, group: GroupModel)
