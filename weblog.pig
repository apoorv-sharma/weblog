register datafu.jar
DEFINE Sessionize datafu.pig.sessions.Sessionize('15m');

data = LOAD '2015_07_22_mktplace_shop_web_log_sample.log' using PigStorage(' ') as (time:datetime,elb:chararray,client:chararray,backend:chararray,request_processing_time:long,backend_processing_time:long,response_processing_time:long,elb_status_code:long,backend_status_code:long,received_bytes:long,sent_bytes:long,random_data:chararray,url:chararray);
filtered_column_data = FOREACH data GENERATE ToUnixTime(time) as long_time:long,FLATTEN(STRSPLIT(client,':',2)) as (ip,port),url;
filtered_column_data = FOREACH filtered_column_data GENERATE long_time,ip as ip:chararray, url;


session_filtered_column_data = FOREACH (GROUP filtered_column_data BY ip) {
  ordered = ORDER filtered_column_data BY long_time;
  GENERATE FLATTEN(Sessionize(ordered))
           AS (long_time,ip,url,sessionId);
}

session_times = FOREACH (GROUP session_filtered_column_data BY (sessionId,ip)) {
    GENERATE group.sessionId as sessionId,
             group.ip as ip,
             (MAX(session_filtered_column_data.long_time) - MIN(session_filtered_column_data.long_time)) as session_length;
}

avg_session = FOREACH (GROUP session_times ALL) {
  GENERATE AVG(session_times.session_length) as avg_session;
}

unique_url_per_session = FOREACH (GROUP session_filtered_column_data BY sessionId) {
	unique_url = DISTINCT session_filtered_column_data.url;
	GENERATE group as sessionId, COUNT(unique_url);
}

total_session_time_per_ip = FOREACH (GROUP session_times BY ip) {
    GENERATE group as ip,SUM(session_times.session_length) as session_length;
}

total_session_time_per_ip = ORDER total_session_time_per_ip by session_length DESC;

top_users_by_total_session = LIMIT total_session_time_per_ip $top_users;

store avg_session into 'avg_session' using PigStorage();
store unique_url_per_session into 'unique_url_per_session' using PigStorage();
store top_users_by_total_session into 'top_engaged_users' using PigStorage();
