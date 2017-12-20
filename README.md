# Aserv
## Overview
It is a distributed system for real-time and low latency scientific event analysis for short-timescale and large field of view sky survey, such as GWAC. The Ground-based Wide-Angle Camera array (GWAC), a part of the SVOM space mission, will search for optical transients of various types by continuously imaging a field-of-view (FOV) of 5000 degrees2 in every 15 seconds. Each exposure consists of20x4kx4k pixels, typically resulting in 20x ~175,600 extracted sources. 

## Aserv's framework
Aserv includes insertion component (astroDB_cache), query engine (astroDB_cache_query) and key-value store (Redis cluster). We also provide a distributed data generator in gwac folder. The insertion component follows “master/slave” mode. It ingests the catalog file and load them into key-value store. Query engine supports three typical analysis methods including probing, listing and stretching.

## Getting Started
We in advance has built Aserv in gwac/ which the insertion component and query engine in gwac/astroDB_cache/. We can run Aserv by:</br>
`$ cd gwac/gwac_dbgen_cluster`
`dfdsfdsa`
