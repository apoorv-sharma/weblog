# weblog
To run this pig script use (The file path is there in the pig script only : 2015_07_22_mktplace_shop_web_log_sample.log)
top_users is the number of engaged users you want to find.

Used 15 minute window for sessionizing.

pig -x local -param top_users=5 -f weblog.pig
