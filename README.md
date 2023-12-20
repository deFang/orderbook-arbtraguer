## Cross Arbitrage

### 任务列表

*. 仓位明细，保证金余额，可用保证金，净值曲线，年化收益(1day, 3day, 7day, 30day)
*. 保证金释放策略：需要动态改变symbol的减仓阈值
*. 部署脚本
*. 新增运行模式: normal, reduce_only, pause(for exchange offline)
*. 使用联合保证金（目前是单币保证金)
*. 研究冰宽limit订单的处理
*. 取消订单的策略: 目前是监听价差，可以考虑监听深度
*. 性能benchmark: orderbook数据时间到下单时间的延时
*. 分离打包程序以支持多个交易所
