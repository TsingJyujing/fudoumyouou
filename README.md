# Domus Analytica

日本房价分析程序

## 设计

在日本购房，与别处（比如中国）是不同的。国内购房，讲究个学区地段小区，随后有按照面积的单价。除非急售，房屋的成交总价大致总不会偏移单价乘以面积过远。
在日本看了些时日（主要还是在SUUMO上）的房子，这里的定价逻辑与国内不同，地段重要，但是重要的是便利程度而非学区，离电车站近的价格更好。
除此之外，房子的年限也是重中之重，新房和老房是截然不同两种价格，相比之下，面积倒没那么重要。2000万可能还不够买一个较繁华地段的一室一厅，但是到偏远些的地方可能买个四室一厅还有余。

本项目用回归对房价数据进行建模分析，以期对购房中我所关注的因素进行量化，来更好的作出决策。

本程序针对以下指标进行分析：

1. 年限（築年月）
2. 房型（間取り）
3. 楼层（所在階/構造・階建）
4. 朝向（向き）
5. 面积（専有面積）
6. 宠物（ペット相談）

还有一个比较抽象的指标：方便程度，需要拆解为以下可量化的变量：
1. 最近的车站的距离
2. 坐车/开车到公司的时间
3. 到最近的超市的距离
4. 到最近的便利店的距离
5. 到最近的邮局的距离
6. ...

还有一些玄学指标：
1. 到最近坟场的距离

回归的目标是

1. 価格
2. 修繕積立金+管理費+修繕積立基金+諸費用

## 具体实现

### 数据收集

房价的数据从SUUMO上收集，使用Google Map的API进行后处理。

除了上述要分析的数据，还需要存储：
1. 该物件的GPS和大致的地址信息
2. 物件名（作为标识）
3. 信息更新日期

为了避免对SUUMO网站造成影响，需要设定爬虫的间隔为正常浏览的间隔，同时，使用筛选条件尽可能减少需要下载的物件。
筛选条件的定义方法是：排除自己一定不会购买的住宅（例如小于40平米/1LDK/价格离谱），排除数据之后对分析的科学性会有所损失，但是足够我使用了。
（如果SUUMO看到这个项目：如果有机会我希望和SUUMO合作，希望可以构建出一个靠谱的房价定价模型）

### 分析方法


## 附录

- [マンションマーケット](https://mansion-market.com/)
- [SUUMO(スーモ)](https://suumo.jp/)
- [スクレイピングで不動産情報取得(SUUMO)(1)](https://qiita.com/kyokohama66/items/30aaacec0bb5c8bd7993)
- [SUUMOの物件情報を自動取得（スクレイピング）したのでコードを解説する。](https://qiita.com/tomyu/items/a08d3180b7cbe63667c9)
- [不動産！マンションマーケットから物件の情報をスクレイピング ](https://note.com/11210858628/n/nd7b69cf8530a)
