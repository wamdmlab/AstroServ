# Aserv
## Overview
It is a distributed system for real-time and low latency scientific event analysis for short-timescale and large field of view sky survey, such as GWAC. The Ground-based Wide-Angle Camera array (GWAC), a part of the SVOM space mission, will search for optical transients of various types by continuously imaging a field-of-view (FOV) of 5000 degrees2 in every 15 seconds. Each exposure consists of20x4kx4k pixels, typically resulting in 20x ~175,600 extracted sources. 
## Aserv's framework
![Alt text](http://p1cfu8w1j.bkt.clouddn.com/Aservimage1.jpg)</br>
Aserv includes insertion component (astroDB_cache), query engine (astroDB_cache_query) and key-value store (Redis cluster). We also provide a distributed data generator in gwac folder. The insertion component follows “master/slave” mode. It ingests the catalog file and load them into key-value store. Query engine supports three typical analysis methods including probing, listing and stretching.

## Getting Started
### Catalog instertion
Aserv is dependent on Ubuntu 14.04, and we in advance has built Aserv in gwac/ which the insertion component and query engine in gwac/astroDB_cache/. When Python 2.7 is installed, we can try to run Aserv's insertion component by:</br>
`# cd gwac/gwac_dbgen_cluster`</br>
`# ./genTemplateStartSquirrel.sh //create template star table`</br>
`# ./sumLineAndDataSize.sh $observationNumber` //1920 times for 8 hours </br>

### Query engine 
We build query engine on Apache Spark 1.6.3 and spark-redis-0.3.2-jar-with-dependencies.jar is needed. </br> Run </br>
`# $SPARK_HOME/bin/spark-submit --class astroDB_query --master spark://$MASTER:7077 --jars $JARS/spark-redis-0.3.2-jar-with-dependencies.jar $HOME/gwac/astroDB_cache/astroDB_cache_query.jar -outputPath show -sliceNum $SLICE_NUM -redisHost $REDIS_IP:POST -ccdNumDefault $CCD_NUM -eachInterval $TIME`
</br> to launch query engine. We provide a parameter example as follow: </br>
`# $SPARK_HOME/bin/spark-submit --class astroDB_query --master spark://$MASTER:7077 --jars $JARS/spark-redis-0.3.2-jar-with-dependencies.jar $HOME/gwac/astroDB_cache/astroDB_cache_query.jar -outputPath show -sliceNum 121 -redisHost $REDIS_IP:POST -ccdNumDefault 20 -eachInterval 15`
#### Queries
We provide 8 queries: </br>
q1 -> probing analysis with PCAG</br>
q2 -> return ids of scientific events</br>
q3 -> listing analysis with SEPI</br>
q4 -> temporal range query with a certain region restriction</br>
q5 -> probing analysis with SEPI</br>
q6 -> stretching analysis</br>
q7 -> listing analysis with PCSE</br>
q8 -> listing analysis with EPI (need EPI's support)</br>

##### Examples for query running  
When the query engine is launched, the aforementioned analytical queries can be executed by inputting:
> Probing analysis:</br>
`query> q1 timeInterval($STARTTIME,$ENDTIME) pixelRound($X,$Y,$R)`</br>
> Listing analysis:</br>
`query> q3 timeInterval($STARTTIME,$ENDTIME) pixelRound($X,$Y,$R)`</br>
> Stretching analysis:</br>
`query> q6 timeInterval($STARTTIME,$ENDTIME) $OBJECT_ID` //an object id who appears scientific event

## Contact us
 <yangchenwo@126.com>
