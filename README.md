[论文地址](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf)

[翻译](https://www.cnblogs.com/luozhiyun/p/15376890.html)

TiDB[实现](https://pingcap.com/zh/blog/tidb-transaction-model)

# 做了什么

第一版简单地实现了percolator分布式事务，简化了论文中的column，因为是一个乐观模型，对于读请求发生的冲突，直接返回报错，并没有像Tidb中实现超时等待（因为时间不够了，之后会补）



secondary节点的异步操作没有实现，以及锁消除，全局时钟，需要做的还有很多。

