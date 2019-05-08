hadoop fs -chmod -R 777 /bili_3-7day
hadoop fs -cat /bili_3-7day/tagInfo.txt
hadoop fs -rmr /bili_3-7day/tagInfo.txt
hadoop fs -touchz /bili_3-7day/tagInfo.txt
hadoop fs -chmod -R 777 /bili_3-7day/tagInfo.txt
hadoop fs -cat /bili_3-7day/tagInfo.txt

hadoop fs -cat /bili_3-7day/last_aid.txt
hadoop fs -rmr /bili_3-7day/last_aid.txt
hadoop fs -put last_aid.txt /bili_3-7day/
hadoop fs -chmod -R 777 /bili_3-7day/last_aid.txt
hadoop fs -cat /bili_3-7day/last_aid.txt

hadoop fs -cat /bili_3-7day/tag_time.txt
hadoop fs -rmr /bili_3-7day/tag_time.txt
hadoop fs -touchz /bili_3-7day/tag_time.txt
hadoop fs -chmod -R 777 /bili_3-7day/tag_time.txt
hadoop fs -cat /bili_3-7day/tag_time.txt
