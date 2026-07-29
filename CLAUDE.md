# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

数据库性能压测的**调度控制器**（benchmark orchestrator），面向 PostgreSQL 系及国产数据库：
IvorySQL、HighGo、Vastbase、Kingbase、GBase 8s、原生 PG。

关键认知：**本程序自己几乎不产生负载**。它做的是编排：

1. 按场景改写 properties 配置文件；
2. 用 `ProcessBuilder` 拉起外部预编译 jar 来跑真实负载；
3. 在每个阶段前后启停 `iostat` / `dstat` 采集 IO 指标；
4. 结束后查询数据库里的 record 表，算出 avgreturn / avgfinish / 100r-sec 等指标并打日志。

外部 jar（**不由本仓库构建**，是黑盒依赖）：

| Jar | 职责 | 读取的配置 |
|---|---|---|
| `tableMigration.jar` | 并发随机读 | `config.properties` |
| `InsertIntoOracle.jar` | 批量 COPY / 逐条 insert 入库 | `l2o.properties` |
| `mockdata.jar` | 生成测试数据文件 | 命令行参数 `-T日期 -D目录 -N天数` |

控制器与这些 jar 之间**唯一的契约就是 properties 文件的键名**。改 `UpdateConfProperties`
的写入逻辑时，必须保证键名与目标 jar 的期望一致 —— 编译期无法校验。

## Build & Run

### Build

**不要用 Maven 构建。** `pom.xml` 未声明任何依赖（只设了 source/target 8 和编码），
而代码依赖 slf4j / log4j2 / 各家 JDBC 驱动，`mvn compile` 必然失败。

实际构建走 IntelliJ artifact：`.idea/artifacts/TestControllerData_jar.xml`
（Build → Build Artifacts → `TestControllerData:jar`），产物在 `out/artifacts/TestControllerData_jar/`。
它把模块输出打成 `TestControllerData.jar`，再把**项目库 `libDBB`** 的全部 jar 释放到同级目录
（`<element id="library" level="project" name="libDBB" />`）。

⚠️ **依赖的真实来源是兄弟目录 `../libDBB/`，不是仓库内的 `lib/`。**
`lib/` 是一份过期快照（缺 `kingbase8-8.6.0.jar`、`tableMigration.jar`），不参与构建。

新增依赖：把 jar 放进 `../libDBB/` → 加入 IntelliJ 项目库 `libDBB` → 在
`MANIFEST.MF` 的 `Class-Path` 里加文件名。artifact 侧不必再逐个登记（早期版本是
20 条 file-copy，现已改为整库引用）。**`Class-Path` 这步不能漏**，否则
`Class.forName()` 加载驱动时抛 `ClassNotFoundException`。

⚠️ **项目库定义 `.idea/libraries/libDBB.xml` 被 `.gitignore` 忽略**（`.gitignore:10`
的 `.idea/libraries/`）。全新 clone 拿不到它，artifact 会因找不到 `libDBB` 库而构建失败 ——
需要在 IntelliJ 里重建该项目库并指向 `../libDBB/`。

`src/main/resources/META-INF/MANIFEST.MF` 的 `Main-Class` 是 `org.testController.Start`，
`Class-Path` 用**裸文件名**，因此依赖 jar 必须与主 jar 同目录平铺。

### Run

必须在**同时包含主 jar、全部依赖 jar 和 properties 文件**的部署目录下运行：

```bash
java -jar TestControllerData.jar
```

指定其它主配置文件：

```bash
java -Dconf=/path/to/allconf.properties -jar TestControllerData.jar
```

**运行环境要求 Linux。** 代码直接 shell 调用 `bash`、`iostat`、`dstat`、`timeout`、
`chmod`、`rpm`、`ss`、`uname`、`find`，以及 `/opt/MegaRAID/storcli/storcli64`。

**CWD 极其敏感**：所有 jar 名、配置文件名、`mock.sh`、输出日志都按裸文件名相对当前工作目录解析。

### Tests

本仓库**没有单元测试**（`src/test/java` 是空目录，无任何测试文件，也没有测试框架依赖）。
这里的「测试」指压测场景本身。

「只跑单个场景」= 在 `allconf.properties` 里只把目标场景开关置 true、其余置 false：

```properties
scene.mock.enabled=false
scene.createPartition.enabled=false
scene1.enabled=false
scene2.enabled=false
scene3.enabled=true      # 只跑场景 3
scene4.enabled=false
scene5.enabled=false
GBbase8s.read.enabled=false
GBbase8s.readAndinsert.enabled=false
```

`TestControllerNew.runAllTests()` 就是按这些开关逐个 `if` 判断的，没有其它筛选机制。

⚠️ `DbManager.isEnabled(key)` **缺省值是 `true`** —— 配置里漏写某个键 = 该场景会跑。
所以关场景要显式写 `=false`，不能靠删行。

仓库里 `allconf.properties` 当前**全部场景开关都是 `false`**（安全默认），
`db.type=kingbase`、`is.install.ivory=false`。跑之前需要按需打开。

## Configuration Architecture

配置分两层，混淆这一点会浪费大量时间：

### `allconf.properties` —— 唯一的真实输入

`DbManager` 静态初始化块加载，路径取 `System.getProperty("conf", "allconf.properties")`。
全项目所有配置读取都经过 `DbManager.getProperty(key)`。加载失败直接抛 `RuntimeException`。

数据库连接采用 **db-type 前缀**约定：`{dbType}.driver` / `.url` / `.host` / `.port` /
`.user` / `.password` / `.database`，由顶层 `db.type` 选定当前库。`.url` 中的
`{host}` / `{port}` / `{database}` 占位符在 `DbManager.getConnection()`、`TestConfig`、
`SceneExecutorNew` 三处各做一次字符串替换。

新增数据库类型时这一整组前缀键必须补齐，否则 `getConnection()` 会 NPE
（`getProperty` 返回 null → `url.replace(...)`）。

### `config.properties` / `l2o.properties` —— 生成的产物，不要手改

这两个文件在每次场景运行时被 `UpdateConfProperties` **整体覆盖重写**：

- `updateConcurrentInsertConfig(...)` → 重写 `l2o.properties`（喂给 `InsertIntoOracle.jar`），
  用 `db.bulkload` 区分 COPY（true）和逐条 insert（false）
- `updateConcurrentReadConfig(queryType)` → 重写 `config.properties`，`MaxThread` **硬编码 100**
- `updateReadConfig(queryType, maxThread)` → 同上，但并发数可传

手工编辑这两个文件不会生效。要改行为，改 `allconf.properties` 或改 `UpdateConfProperties`。

## Execution Flow

```
Start.main()
  ├─ 若 db.type=ivory 且 is.install.ivory=true → CheckDatabaseInstall（rpm 检测 + 自动装库）
  ├─ CheckHardware.checkStorageHealth()   ← 硬盘/RAID 预检，不通过则 System.exit(1)
  └─ TestControllerNew(new TestConfig(dbType)).runAllTests()
       ├─ DatabaseFactory.getDatabase(config) → DatabaseInface 实现
       └─ 按 scene*.enabled 开关逐个 new ScenarioN(config).run(db)
```

各场景语义：

| 场景 | 开关 | 做什么 |
|---|---|---|
| MockData | `scene.mock.enabled` | 生成 `mock.sh` → 执行 → 轮询等待 N 个 >4080MB 的文件 |
| 建分区表 | `scene.createPartition.enabled` | 调 `db.createPartitionTable()`，按天建范围分区 |
| Scenario1 | `scene1.enabled` | `COPY ... STDIN` 批量入库 + 建 4 个分区索引（usernum/imei/imsi/lai,ci） |
| Scenario2 | `scene2.enabled` | 对 3 个日期做 `CREATE TABLE AS SELECT DISTINCT usernum` → `tb_usernum_list1/2/3` |
| Scenario3 | `scene3.enabled` | 100 并发随机读 `timeout.read.hour` 小时，结果落 `tb_test_record_sql1` |
| Scenario4 | `scene4.enabled` | 逐条 insert 入库，**固定循环 3 轮**，监控间隔用 `monitorInterval.60` |
| Scenario5 | `scene5.enabled` | 场景 3 + 场景 4 并发混合 |
| GBase 读 | `GBbase8s.read.enabled` | GBase 8s 专用：50/100/200 并发 × 精确/范围/排序/开窗 查询矩阵 |
| GBase 读写 | `GBbase8s.readAndinsert.enabled` | GBase 8s 专用并发读写 |

**Scenario5 的结束条件由「写」决定**：`future2.get()` 等入库任务完成，`finally` 里
`executor.shutdownNow()` 中断读任务；`JavaProcessExecutor` 捕获 `InterruptedException`
后调 `process.destroyForcibly()` 杀掉子进程。修改 Scenario5 并发控制时务必保住这条中断链路。

场景间有**隐式顺序依赖**，不能任意单独跑：Scenario2 依赖 Scenario1 已灌好数据；
Scenario3 依赖 Scenario2 产出的 `tb_usernum_list1`（会被 rename 成 `tb_usernum_list`）。

## 新增数据库方言

方言分支散落在 3 处代码 + 2 处装配，加一种库要**全部**照顾到：

**代码**

1. `dataBase/DatabaseFactory.java` — switch 分支
2. 新建 `dataBase/XxxDatabase implements DatabaseInface` —— **建分区表 DDL 现在完全由各实现自己负责**
   （集中式的 `PatitionTableCreator` 已删除）
3. `testController/PrepareSecnarioEnvironment.checkTableIsExist()` — 判断表是否存在的元数据查询

**装配**（漏掉这两步代码能编译但运行必炸）

4. `allconf.properties` 补一整组 `{dbType}.*` 连接键
5. 驱动 jar 放入 `../libDBB/` 并加入项目库 `libDBB`，**并**在 `MANIFEST.MF` 的
   `Class-Path` 里加文件名

> **Kingbase 是完整的参考样例**：`KingbaseDatabase` + `DatabaseFactory` 分支 +
> `checkTableIsExist` 分支 + `allconf.properties` 的 `kingbase.*` 键 +
> `libDBB/kingbase8-8.6.0.jar` + `MANIFEST.MF` 的 `Class-Path` —— 五处齐备。
> 照着它加新库不容易漏。

各库差异：

- **分区索引**：ivory / pg 用 `createPartitionIndexesPgIvory` —— `DO $$` 匿名块遍历
  `pg_inherits` 给**每个子分区**单独建索引；highgo / vastdata / kingbase / Oracle 用
  `createPartitionIndexesOnTable` —— 直接对主表 `CREATE INDEX`（Oracle 追加 `LOCAL`）；
  GBase 8s 用 `createPartitionIndexesGBase8s`
- **建分区**：ivory / kingbase 用 `PARTITION BY RANGE` + 匿名块循环建子分区；
  highgo / vastdata 用内联 `PARTITION ... VALUES LESS THAN` 列表；
  GBase 8s 用 `fragment by range(...) interval(...)` 并显式指定 dbspace
- **判断表存在**：pgdb/ivory/kingbase `to_regclass` · vastdata `pg_catalog.all_tables` ·
  highgo `pg_catalog.pg_tables` · Oracle `dba_tables` · gbasedbt `systables`
- **`dbType` 大小写不敏感**（一律 `equalsIgnoreCase` / `toLowerCase`），
  但配置键前缀**大小写敏感**

## 已知限制与陷阱

- **`PostgresDatabase` 三个方法体为空 —— 有意为之，pgdb 类型暂未实现。**
  `db.type=pgdb` 时建分区表和 Scenario1 批量入库均无操作。
- **`DatabaseInface` 只覆盖 3 个操作**（`createPartitionTable` / `copyData` / `createPartIndexes`）。
  场景 2/4/5 对应的接口方法在源码里是注释状态，这些场景直接调 `org.testController.*`
  的静态工具类，绕过了策略体系。
- **`CheckHardware` 是 fail-open（已知，待修）**：`storcli64` 缺失或路径不对时 `execute()`
  返回错误提示字符串，不含任何异常关键字，预检因此判定为「通过」。
  另：`checkStorageHealth()` 是 `static`，却用 `new CheckHardware().checkStorageHealth()` 调用。
- **SQL 全部字符串拼接**，表名来自配置。这是内网压测工具的既有形态，改动时保持一致即可，
  但不要把外部不可信输入接进来。
- **仓库没有 log4j2 配置**（`src/main/resources` 下只有 `MANIFEST.MF`），日志配置需由部署目录提供。
- **类名拼写错误**：`PrepareSecnarioEnvironment`、`GBaseRandomReadSecnario`、
  `GBaseReadAndInsertSecnario` 里是 `Secnario` 而非 `Scenario`，搜索时注意。
- **`Mockdata.waitForVaildFiles` 的超时判断无效**：`countTimes > timeOut` 只在进入 `while` 前
  判断一次，循环内部不再检查，实际是无限轮询。
- **`SceneExecutorNew` 与 `TestConfig` 字段重复**（前者少一个 `Connection`）。
  `UpdateConfProperties` 用前者取连接信息。改连接相关字段时两边都要动。
- 日志文件名（含 `iostat`/`dstat` 采集结果）直接写在 CWD，形如
  `Scenario1.copy.iostat.600.log`、`Scenario3_read_out_20250725_092415.log`。
