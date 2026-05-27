# Bilibili 平台数据湖仓项目实施方案（参考 Reddit-Streaming-Lakehouse）

> 目标：仿照当前 Reddit Streaming Lakehouse 项目，构建一个 **面向 Bilibili 内容生态** 的端到端数据湖仓与分析平台，覆盖数据采集、ETL、数仓建模、可视化与数据挖掘。

---

## 1. 项目目标与边界

### 1.1 总体目标

构建一套可持续运行的数据平台，支持：

1. 从 Bilibili 获取视频、UP 主、评论、弹幕等多维数据；
2. 通过批处理 + 准实时任务完成清洗、标准化与主题建模；
3. 建立 Lakehouse 分层（Bronze/Silver/Gold）和星型模型；
4. 在 BI 和 Notebook 中完成运营分析、内容洞察与挖掘建模。

### 1.2 业务问题（建议先定义）

- 哪些分区（番剧、科技、生活等）在不同时间段增长最快？
- 哪类视频更容易获得“高互动率（播放→点赞/投币/收藏）”？
- 弹幕/评论情绪与视频热度之间是否存在显著相关性？
- 哪些 UP 主具备“持续爆款”能力？
- 用户在不同主题视频中的行为偏好（评论长度、情绪、互动密度）如何？

---

## 2. 数据源与采集设计

> **原则**：优先使用公开、合规接口；遵守平台 robots、频控、隐私与法律要求。

### 2.1 数据源分层

#### A. 官方开放接口（优先）
- Bilibili OpenAPI（如可申请到权限）
- 作用：用户信息、视频基础属性、互动统计等结构化信息
- 优点：稳定、字段语义清晰
- 风险：权限/限流约束

#### B. Web 公开接口 / 页面解析（补充）
- 视频详情页、搜索结果页、排行榜页、分区页
- 作用：补齐 OpenAPI 不提供的数据
- 风险：反爬策略、字段变更、HTML 结构变动

#### C. 历史离线数据源（可选）
- Kaggle、GitHub 开源样本、团队历史归档
- 作用：冷启动训练集或历史回填

### 2.2 采集对象清单（核心）

#### 2.2.1 视频实体（Video）
- 唯一键：`bvid` / `aid`
- 字段：标题、简介、分区、标签、发布日期、时长、清晰度、版权类型
- 指标：播放、点赞、投币、收藏、分享、评论数、弹幕数

#### 2.2.2 UP 主实体（Creator）
- 唯一键：`mid`
- 字段：昵称、粉丝数、关注数、认证信息、等级、签名
- 指标：总稿件数、总播放（如可得）、近 30 天发稿频率

#### 2.2.3 评论实体（Comment）
- 唯一键：`rpid`
- 字段：评论文本、发布时间、楼层、父子关系、点赞数、用户名（脱敏）
- 指标：情绪值、关键词、主题标签

#### 2.2.4 弹幕实体（Danmaku）
- 唯一键：`(cid, progress, content_hash)`
- 字段：弹幕文本、发送时间点、颜色、字号、模式、时间戳
- 指标：情绪值、脏词标记、热词

#### 2.2.5 榜单/搜索快照（Snapshot）
- 日榜、周榜、分区热门、关键词搜索 TopN
- 用于构建“时间序列热度变动”分析

### 2.3 采集策略

#### 批处理（日更）
- 每天固定时间抓取：
  - 新发布视频（按分区/关键词）
  - 存量视频增量指标（播放、点赞等）
  - 榜单快照

#### 准实时（5~15 分钟）
- 对重点视频池（热点候选）轮询更新互动指标

#### 增量机制
- 高水位（watermark）：`publish_time`、`last_update_time`
- 幂等写入：`upsert + 去重键`
- 异常重试：指数退避 + 死信队列（DLQ）

### 2.4 合规与风控

- 控制请求频率、随机间隔、重试上限
- User-Agent 与请求头规范化
- 敏感字段脱敏：用户名、UID 哈希化
- 数据保留与删除策略（如 90/180 天滚动）

---

## 3. 技术栈与环境配置

## 3.1 推荐技术栈（对齐当前项目风格）

- **语言**：Python 3.10+
- **采集层**：`httpx` / `requests` + `playwright`（仅动态页面必要时）
- **消息队列（可选）**：Kafka
- **计算引擎**：PySpark
- **表格式（Lakehouse）**：Apache Iceberg（推荐）或 Delta Lake
- **对象存储**：MinIO / S3
- **元数据管理**：Hive Metastore / Nessie
- **查询引擎**：Trino
- **编排调度**：Airflow
- **可视化**：Superset
- **数据挖掘**：Jupyter + scikit-learn + transformers（中文模型）

### 3.2 Docker 化环境（建议目录）

```text
bilibili-lakehouse/
├─ docker/
│  ├─ compose.yaml                 # minio + spark + trino + hive + airflow + superset + kafka
│  ├─ spark.compose.yaml
│  ├─ airflow.compose.yaml
│  ├─ trino.compose.yaml
│  └─ superset.compose.yaml
├─ ingestion/
│  ├─ crawlers/
│  ├─ parsers/
│  └─ jobs/
├─ src/
│  ├─ bronze/
│  ├─ silver/
│  ├─ gold/
│  └─ features/
├─ dags/
├─ notebook/
└─ docs/
```

### 3.3 Python 依赖建议

- 数据采集：`httpx`, `tenacity`, `pydantic`, `orjson`, `beautifulsoup4`, `lxml`
- 数据处理：`pyspark`, `pyarrow`, `pandas`
- NLP：`jieba`, `snownlp`（轻量）或 `transformers`, `sentence-transformers`
- 挖掘：`scikit-learn`, `xgboost`, `lightgbm`
- 可视化分析：`matplotlib`, `seaborn`, `plotly`

### 3.4 基础配置项

- `.env`：AK/SK、Kafka Topic、MinIO Bucket、Trino 连接、Airflow 变量
- `conf/source.yaml`：分区、关键词、抓取频率、限流参数
- `conf/schema/*.json`：各层 schema 版本

---

## 4. 数据湖仓分层与 ETL Pipeline

## 4.1 分层标准

### Bronze（原始层）
- 存放原始 JSON / HTML 解析结果
- 只做轻度清洗（加采集时间、来源、trace_id）
- 分区建议：`dt=YYYY-MM-DD/hour=HH/source=...`

### Silver（标准层）
- 统一字段命名、类型转换、空值处理、去重
- 拉平嵌套结构（如标签数组、评论层级）
- 增加衍生字段：互动率、发布时间粒度、文本长度

### Gold（业务层）
- 面向分析场景的主题宽表与聚合表
- 例：视频表现主题、UP 主画像主题、评论情绪主题、分区热度主题

## 4.2 ETL 任务设计（按 DAG）

1. `ingest_video_metadata`：采集视频基础信息写 Bronze
2. `ingest_comments_danmaku`：采集评论与弹幕写 Bronze
3. `silver_video_cleaning`：视频标准化与去重
4. `silver_text_processing`：评论/弹幕分词、清洗、情绪打分
5. `gold_fact_build`：构建事实表、聚合表
6. `feature_store_build`：构建训练特征表
7. `quality_check`：质量校验（完整性、唯一性、异常波动）

## 4.3 数据质量规则（关键）

- 主键唯一：`bvid`, `mid`, `rpid`
- 业务范围：播放/点赞等应 `>=0`
- 时间合理：`publish_time <= collect_time`
- 文本质量：空文本比例阈值、乱码比例阈值
- 波动监控：日采集量较近 7 日均值偏差 > X% 告警

---

## 5. 数仓模型设计（星型模型）

## 5.1 维度表（Dimension）

### `dim_video`
- `video_sk`（代理键）, `bvid`, 标题、分区、时长、发布时间、标签

### `dim_creator`
- `creator_sk`, `mid`, 昵称、认证、粉丝等级、账号标签

### `dim_time`
- `date_sk`, 年、季、月、周、日、小时、节假日标记

### `dim_category`
- `category_sk`, 一级分区、二级分区

### `dim_keyword`
- `keyword_sk`, 关键词、主题归类（科技/娱乐/学习等）

## 5.2 事实表（Fact）

### `fact_video_daily`
- 粒度：视频-天
- 指标：播放、点赞、投币、收藏、分享、评论数、弹幕数、新增粉丝估计

### `fact_comment`
- 粒度：评论级
- 指标：点赞数、情绪得分、是否回复、是否高赞

### `fact_danmaku`
- 粒度：弹幕级
- 指标：发送时间点、情绪分类、关键词标签

### `fact_hot_snapshot`
- 粒度：榜单-抓取时间
- 指标：排名、热度、上升/下降位次

## 5.3 拉链与快照策略

- UP 主属性建议 SCD2（粉丝量段位、认证变化）
- 视频统计采用快照事实表（每日一快照）

---

## 6. 可视化方案（Superset）

## 6.1 仪表盘设计

### Dashboard A：内容全景看板
- 日活视频数、总播放量、总互动量
- 各分区投稿与互动趋势
- 热门视频 TOPN

### Dashboard B：UP 主增长看板
- 粉丝增长分层
- 发稿频次 vs 平均互动率散点图
- 头部/腰部/长尾 UP 占比

### Dashboard C：评论与弹幕舆情看板
- 情绪分布（正/中/负）
- 热词云、主题占比
- 时间轴情绪波动（事件响应）

### Dashboard D：热点追踪看板
- 榜单排名变化（折线）
- 爆款视频生命周期（首发后 1/3/7/14 天）

## 6.2 核心可视化对象

- 对象 1：视频热度漏斗（播放→点赞→投币→收藏）
- 对象 2：分区内容供给与需求对比
- 对象 3：评论情绪与互动率耦合图
- 对象 4：UP 主稳定爆款识别矩阵

---

## 7. 数据挖掘与建模计划

## 7.1 课题一：视频热度预测（回归/分类）

- 目标：预测视频发布后 24h/72h 播放量级别
- 特征：标题长度、标签数、发布时间段、UP 主历史表现、早期互动速度
- 模型：LightGBM / XGBoost
- 评估：RMSE（回归）或 F1/AUC（分类）

## 7.2 课题二：评论与弹幕主题发现

- 方法：TF-IDF + KMeans / BERTopic
- 输出：主题词、主题热度趋势、主题-分区映射

## 7.3 课题三：情绪驱动分析

- 方法：中文情感模型（如 RoBERTa 中文情感分类）
- 分析：情绪分数与视频涨粉/互动之间的相关性

## 7.4 课题四：UP 主分群

- 方法：RFM 变体（发稿频率F、互动质量M、稳定性R）+ 聚类
- 输出：运营分群（潜力型/稳定型/爆发型/沉寂型）

---

## 8. 项目实施里程碑（12 周样例）

## Phase 0（第 1 周）：方案与合规确认
- 明确采集范围与合规要求
- 完成字段字典与数据标准

## Phase 1（第 2-4 周）：采集与 Bronze
- 打通采集链路（视频、评论、弹幕、榜单）
- 形成可回放的原始数据层

## Phase 2（第 5-7 周）：Silver/Gold 与数据质量
- 完成标准化、去重、情绪与主题基础处理
- 构建事实表/维表，接入质量监控

## Phase 3（第 8-10 周）：BI 看板
- 上线 3~4 个核心业务看板
- 与业务问题一一映射

## Phase 4（第 11-12 周）：挖掘模型与复盘
- 完成热度预测、UP 主分群等 MVP
- 输出项目复盘与下一期路线图

---

## 9. 交付物清单

1. 数据采集模块（含配置化抓取策略）
2. Airflow DAG（端到端）
3. Bronze/Silver/Gold 表结构与建表脚本
4. 数据质量规则与告警策略
5. Superset 仪表盘（不少于 3 套）
6. Notebook 挖掘分析报告
7. 项目文档（部署、运行、扩展指南）

---

## 10. 风险与应对

- **接口变更风险**：抽象解析层，配置驱动字段映射
- **反爬封禁风险**：降低并发 + 缓存 + 代理池（合规前提）
- **数据漂移风险**：建立 schema registry 与质量报警
- **模型失效风险**：定期重训与特征漂移监测

---

## 11. 建议的首批 MVP 范围（强烈建议）

为了快速落地，第一版只做：

- 分区：科技 + 生活 + 游戏（3 个分区）
- 数据对象：视频 + 评论（先不做弹幕）
- 频率：日批
- 可视化：1 个总览看板 + 1 个情绪看板
- 挖掘：1 个热视频预测模型

MVP 稳定后，再扩展弹幕、实时链路与复杂主题模型。

---

## 12. 与现有 Reddit 项目的对应关系（迁移映射）

- Reddit `posts` ≈ B站 `videos`
- Reddit `comments` ≈ B站 `comments`（结构类似，可复用情绪清洗思路）
- Reddit `subreddit` ≈ B站 `category/partition`
- 现有 Spark + Airflow + Trino + Superset 组件可直接复用

这意味着你可以优先复用当前仓库中的：

- 容器编排思路（docker compose）
- Bronze/Silver/Gold 分层代码结构
- Notebook 分析流程模板

再把源数据适配层换成 Bilibili 的采集器即可。
