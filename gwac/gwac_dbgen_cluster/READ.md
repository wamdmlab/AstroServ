分布式GWAC数据生成与入库系统

该系统借助于redis cluster和redis-cluster-tool，使用前需搭建redis cluster，并将redis-cluster-tool文件夹移至$REDIS_HOME下，并将redis cluster配合为高可用模式。

功能描述：该系统为分布式系统，将gwac交叉验证程序作为后台程序，每15秒产生一次数据将数据文件地址发送给后台程序，后台程序对其入库，能够实现入库在规定时间内入库没有完成，构建一级缓存暂存未完成数据，可继续进行下一次入库，该模式是一种非阻塞的入库方式。

其中一个节点为master，剩余为slave节点，master节点下$GWAC_HOME/gwac_dbgen_cluster/nodehostname配置slave节点的主机名。

主要操作流程：
（1） 配置好nodehostname后，将gwac文件夹复制到所有slave节点。
（2） ./genTemplateStartSquirrel.sh，启动交叉验证和入库程序Squirrel，如果已经有模板星表，则不生成新的模板星表，否则生成新的模板星表。在这个过程之前可以使用deleteTemplateTable.sh清空模板星表，以产生新的，或使用clearLogs.sh清空master和slave的日志文件，需要注意的是，master节点默认每5个产生周期(一个产生周期默认15s)和运行结束前写一条日志信息，下次运行时，会读取上一条日志，并继续在其上累加，如已产生的文件总量，已生成的星总行数等信息（设计目的为，gwac只有晚上开，白天停，每次晚上开启，能够继续记录gwac从第一次开启累计到现在的总量），如果想要继续累加一定不能删除master下的log文件。
（3）./sumLineAndDataSize.sh $NUM
启动总程序，开始生成星表，并入库，$NUM是生成次数，默认一次15秒，其余参数可以进入sumLineAndDataSize.sh修改。在运行期间可以在任意能连上集群的节点执行：
      ./stopGen.sh $STOP,
	  停止集群，$STOP可以取两个值，一个为stop！代表下一个产生周期(15s)后，系统正常停止，force代表，现在立刻停止系统，用于在系统出现故障时使用。
      ./addAbnormalStar.sh $HOST $STARNUM，用于在$HOST节点上产生$STARNUM个异常星，异常星的默认标准为亮度大于50.
      ./getSumData.sh 用于获取当前系统，总的累计数，默认包括总星行数，数据总量，数据戳。
      