2017年4月28日
当前版本为gwac_dbgen_cluster 2.0版本
版本特性：
1、更新Squirrel写缓存系统为astroDB_cache, astroDB_cache系统支持4种写模式和两种存储格式，两种存储格式为KEY-LIST和KEY-ZSET，为了方便查询，我们默认使用KEY-ZSET结构，4种写redis模式分别为：
 "perLineWithCache"： ：每颗星一张逻辑表（即对应一条list或一个zset），为了缓解网络延迟影响，引入在astroDB_cache内部增加本地内存缓存，每次仅写部分星的数据到Redis，引入问题是查询延迟，因为本地缓存数据没有及时写入Redis
"perLineWithNoCache"：每颗星一张逻辑表（即对应一条list或一个zset），每次写全部星的数据到Redis
"starTableAsaString":每个CCD一张逻辑表（即对应一条list或一个zset）。
"perBlockAsaString":通过地理位置（x，y），对星聚簇成blockNum块，每一块一张逻辑表（即对应一条list或一个zset），能有效缓解网络延迟的性能下降，并不会引入查询延迟（默认支持）

2、引入随机异常星生成算法和基于异常星的间隔索引创建算法，生成的星不在是单点异常，而是阶段异常行为如下
异常星         亮度
abstar1     ..........       ............
abstar2            ..........  ..  ......

异常星的异常亮度按照，perLineWithNoCache模式存储。

3、更新gwac_dbgen_cluster的sumLineAndDataSize.sh可并发向多台机器发送运行指令。更新sendStaDataPerNode.sh为常驻进程，而不用每次都要ssh重新启动在运行，以减少不必要的开销。当sumLineAndDataSize.sh运行完指定次数后，sendStaDataPerNode.sh和astroDB_cache将结束。

4、支持不同节点生成不同天区的星表，每个ccd星表有自己ra和dec中心，为了支持该特性，修改nodehostname内部，每一行位hostname-ra0-dec0，ra0和dec0为每个ccd星表的中心赤经赤尾。

5、当前所有CCD产生的xy坐标都在一个大的xy坐标系下，因此根据范围查询时能对多个ccd观测范围查询，即搜索范围如果大于一个ccd的观测范围是支持的。