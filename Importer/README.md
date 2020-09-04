# FormatDataBuilder

从MySQL以固定格式导出数据

# ImporterToJanusgraph

读入固定格式数据，并导入到Janusgraph中
导入顶点时，数据中的id与Janusgraph生成的直接索引id会被构造为键值对记录在MySQL中

# MysqlToRedis

记录在MySQL中的键值对会被导入到Redis中，方便导入边时对给定顶点的快速索引
