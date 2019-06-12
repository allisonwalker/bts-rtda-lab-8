package models

import java.sql.Timestamp


case class VenueModel(venue_name: String, lon: Double, lat: Double, venue_id: String)

case class MemberModel(member_id: Long, photo: String, member_name: String)

case class Event(event_name: String, event_id: String, time: Long, event_url: String)

case class GTopicModel(urlkey: String, topic_name: String)

case class GroupModel(group_topics: Array[GTopicModel], group_city: String, group_country: String, group_id: Long, group_name: String,  group_lon: Double, group_urlname: String, group_state: String, group_lat: Double )

case class MeetupModel(venue: VenueModel, visibility: String, response: String, guests: Long, member: MemberModel, rsvp_id: Long,  mtime: Timestamp, group: GroupModel)
