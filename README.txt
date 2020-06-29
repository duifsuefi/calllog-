一、
dianxin：数据生产
kafka_hbase：数据采集
hbase_mysql：数据分析
flume-kafka：flume配置文件
hase_mysql中，CountDriver.java是分析年月日的，IntymacyDriver.java和PreIntymacyDriver.java是分析亲密度的。通过IntymacyDriver.java预先合并数据，PreIntymacyDriver.java实现业务逻辑。

二、
初始数据格式：
主叫号码，主叫人，被叫号码，被叫人，时间，时间戳，通话时长
13162629996,赵浩歌,13042089546,刘墉,2020-06-19 03:04:59,1592550299448,5011

HBase的RowKey格式
最后加了一位数，1或者0。1表示13162629996 浩歌 为主叫。0表示13162629996 浩歌 为被叫
13162629996 浩歌 3042089546 墉 020-06-19 03:04:59 592550299448 5011 1/0

三、
MySQL表结构
1.tb _contacts表 
id              int       主键 自增
telephone       varchar
name            varvhar

保存用户手机号码和名字


2.tb_dimension_date表
id            int         主键 自增
year          int 
month         int
day           int

保存日期，年维度时month和day为-1，月维度时day为-1

3.tb_call表
id_date_contact       varchar 主键，值为 用户id_日期id
id_date_dimension     int     日期id
id_contact            int     用户id
call_sum              int     通话次数
call_duration_sum     int     通话总时长


4.tb_intimacy表
id                  int      主键，自增
intimacy_rank       int      亲密度排名
contact_id1         int      当前统计人
contact_id2         int      与当前统计人关系亲密的人
call_count          int      通话次数
call_duration_count int      通话总时长


