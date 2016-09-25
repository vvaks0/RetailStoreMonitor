package com.hortonworks.iot.retail.events;

public class SocialMediaEvent {
		private String eventTimeStamp;
		private String statement;
		private String ipAddress;
		private String region;
		private String latitude;
		private String longitude;
		private String sentiment;
		
		public String getEventTimeStamp() {
			return eventTimeStamp;
		}
		public void setEventTimeStamp(String eventTimeStamp) {
			this.eventTimeStamp = eventTimeStamp;
		}
		public String getIpAddress() {
			return ipAddress;
		}
		public void setIpAddress(String ipAddress) {
			this.ipAddress = ipAddress;
		}
		public String getLatitude() {
			return latitude;
		}
		public void setLatitude(String latitude) {
			this.latitude = latitude;
		}
		public String getLongitude() {
			return longitude;
		}
		public void setLongitude(String longitude) {
			this.longitude = longitude;
		}
		public String getStatement() {
			return statement;
		}
		public void setStatement(String statement) {
			this.statement = statement;
		}
		public String getSentiment() {
			return sentiment;
		}
		public void setSentiment(String sentiment) {
			this.sentiment = sentiment;
		}
		public String getRegion() {
			return region;
		}
		public void setRegion(String region) {
			this.region = region;
		}
}