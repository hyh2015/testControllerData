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

被调度的程序（**源码不在本仓库**，以构建产物形式消费）：

| 程序 | 主类 | 职责 | 读取的配置 | 交付形式 |
|---|---|---|---|---|
| tableMigration | `com.test.Test` | 并发随机读 | `config.properties` | 打进本 fat jar |
| InsertIntoOracle | `com.s1.l2o.Start` | 批量 COPY / 逐条 insert 入库 | `l2o.properties` | 打进本 fat jar |
| mockdata | `com.s1.mock.MockData` | 生成测试数据文件 | 命令行参数 `-T日期 -D目录 -N天数` | 独立 `mockdata.jar` |

前两个已随本项目打进同一个 fat jar，因此 `TestConfig` 里存的是**主类名**而非 jar 文件名，
由 `JavaProcessExecutor` 以 `java -cp <自身jar> <主类>` 自调用拉起。

**仍然走子进程而不是直接方法调用**，是因为这两个程序依赖进程边界：

- 配置读在静态初始化块（`PropertiesUtil`）/ 单例构造函数（`PropertyManager`）里，每个 JVM 只读一次；
  而调度器每个场景前都会重写这两个 properties，同进程复用会让第二个场景之后静默沿用旧配置。
- 随机读的 `TestThread1~6` 是 `while(true)` 且把 `catch` 写在循环体**内部**，中断异常会被吞掉，
  只能靠杀进程停下 —— 场景 3 靠 `timeout N`，场景 5 靠 `shutdownNow()` 触发
  `InterruptedException` 再 `destroyForcibly()`。

控制器与这些程序之间**唯一的契约就是 properties 文件的键名**。改 `UpdateConfProperties`
的写入逻辑时，必须保证键名与目标程序的期望一致 —— 编译期无法校验。
另外它们都**忽略命令行传入的配置文件名**，只认当前工作目录下的固定文件名。

## Build & Run

### Build

用 Maven 构建，产出单个 fat jar。项目自带 Maven Wrapper，构建机只需 JDK 8 或 11
（`source/target=8`，产物为 Java 8 字节码，在 8 与 11 上都能跑）：

```bash
./install-libs.sh          # libDBB 不在 ../libDBB 时用 LIB_DIR=... 指定
./mvnw clean package       # 产物：target/TestControllerData.jar（约 22 MB）
```

`install-libs.sh` 把 `../libDBB/` 下的 20 个 jar 用 `install:install-file` 灌进**项目内**的
Maven 文件仓库 `lib-repo/`（已 gitignore）。装在项目内而非 `~/.m2`，是为了不覆盖机器上
其他项目在用的同名构件；灌完之后构建完全离线。

⚠️ **依赖的真实来源是兄弟目录 `../libDBB/`，不是仓库内的 `lib/`。**
`lib/` 是一份过期快照（缺 `kingbase8-8.6.0.jar`、`tableMigration.jar`），不参与构建。

新增依赖：把 jar 放进 `../libDBB/` → 在 `install-libs.sh` 的 `ARTIFACTS` 表里加一行 →
在 `pom.xml` 里加 `<dependency>`。

⚠️ **`pom.xml` 里 JDBC 驱动的声明顺序不可随意调整。** Vastbase 和 HGDB 的驱动都是
PostgreSQL 驱动的分支，与 `postgresql` 存在同名类（分别重叠 298 个和 13 个），
fat jar 里同名类只能活一份，由声明顺序决定。现在让 `postgresql` 排最前，
与改造前三个 MANIFEST 的 `Class-Path` 顺序一致（那时也是 PG 副本胜出）。详见 pom 内注释。

shade 配置中两处不能删：`ServicesResourceTransformer`（12 个依赖带
`META-INF/services/`，不合并会让 slf4j / log4j2 的 Provider 被覆盖、日志静默失效）、
以及对两个程序包 `log4j2.xml` 的排除（同名互相覆盖，改由本项目提供三份配置）。

### Run

必须在**包含 `allconf.properties` 的部署目录**下运行（`mockdata.jar` 按需同放）：

```bash
java -jar TestControllerData.jar
```

指定其它主配置文件：

```bash
java -Dconf=/path/to/allconf.properties -jar TestControllerData.jar
```

**运行环境要求 Linux。** 代码直接 shell 调用 `bash`、`iostat`、`dstat`、`timeout`、
`chmod`、`rpm`、`ss`、`uname`、`find`，以及 `/opt/MegaRAID/storcli/storcli64`。

**CWD 极其敏感**：配置文件名、`mock.sh`、输出日志都按裸文件名相对当前工作目录解析。
自调用的子进程继承父进程的工作目录（`ProcessBuilder` 默认行为），因此它们也在同一目录下
找 `config.properties` / `l2o.properties`。

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

`DbManager.isEnabled(key, defaultValue)` **强制每个调用点显式传缺省值**，不提供
隐式默认的无参重载。场景/安装类开关（`scene*.enabled`、`is.install.ivory`）传 `false`——
漏配置不该意外多跑一个耗时的场景或触发一次没人要求的装库；安全检查类开关
（`hardware.check.enabled`）传 `true`——漏配置时默认去检查，比默默跳过更安全。
新增开关调用点时记得想清楚这个键该属于哪一类。

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

- `updateConcurrentInsertConfig(...)` → 重写 `l2o.properties`（喂给入库程序 `com.s1.l2o.Start`），
  用 `db.bulkload` 区分 COPY（true）和逐条 insert（false）
- `updateConcurrentReadConfig(queryType)` → 重写 `config.properties`，`MaxThread` **硬编码 100**
- `updateReadConfig(queryType, maxThread)` → 同上，但并发数可传

手工编辑这两个文件不会生效。要改行为，改 `allconf.properties` 或改 `UpdateConfProperties`。

## Execution Flow

```
Start.main()
  ├─ is.install.ivory=true 时：db.type 必须是 ivory，否则直接抛 IllegalStateException 终止；
  │  是 ivory 则走 CheckDatabaseInstall（rpm 检测 + 自动装库）
  │  ← 装库为 fail-fast：架构未知/包名未配置/脚本改写失败/安装未出成功标记/
  │     装完状态检查不过，任一不成立都抛 IllegalStateException 终止
  ├─ hardware.check.enabled=true（缺省）时：CheckHardware.checkStorageHealth()
  │  ← 硬盘/RAID 预检，storcli 不可用或检出异常均不通过，System.exit(1)
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
5. 驱动 jar 放入 `../libDBB/`，**并**在 `install-libs.sh` 的 `ARTIFACTS` 表和
   `pom.xml` 的 `<dependencies>` 里各加一条

> **Kingbase 是完整的参考样例**：`KingbaseDatabase` + `DatabaseFactory` 分支 +
> `checkTableIsExist` 分支 + `allconf.properties` 的 `kingbase.*` 键 +
> `libDBB/kingbase8-8.6.0.jar` + `install-libs.sh` 与 `pom.xml` 的声明 —— 五处齐备。
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

- **`pgdb`（原生 PG，面向 PG 17）与 `ivory` 共用同一套 DDL 与建索引方式**，
  因为 IvorySQL 本身是 PG 分支 —— 保持一致两者的压测耗时才可比。
  建索引特意沿用逐分区方式（`createPartitionIndexesPgIvory`）而非 PG 11+ 的
  「对主表 CREATE INDEX 自动下推」，后者会在整个建索引期间持有主表 ACCESS EXCLUSIVE 锁。
- **`DatabaseInface` 只覆盖 3 个操作**（`createPartitionTable` / `copyData` / `createPartIndexes`）。
  场景 2/4/5 对应的接口方法在源码里是注释状态，这些场景直接调 `org.testController.*`
  的静态工具类，绕过了策略体系。
- **`CheckHardware` 是 fail-closed**：`storcli64`（路径硬编码 `/opt/MegaRAID/storcli/storcli64`）
  不可执行、执行失败或退出码非 0 时预检一律不通过 —— 「没检成」不等于「检查通过」。
  非 MegaRAID 机器需用 `hardware.check.enabled=false` 跳过，否则程序启动即退出。
- **SQL 全部字符串拼接**，表名来自配置。这是内网压测工具的既有形态，改动时保持一致即可，
  但不要把外部不可信输入接进来。
- **日志配置有三份**（`log4j2.xml` 调度器 / `log4j2-reader.xml` 随机读 / `log4j2-writer.xml` 入库）。
  三个程序同处一个 fat jar，无法再靠同名 `log4j2.xml` 区分，后两份由 `JavaProcessExecutor`
  启动子进程时用 `-Dlog4j.configurationFile` 指定。加新的被调度程序时记得同步这张映射表。
- **ivory 的 rpm 包必须与 `setupivory.sh` 同目录**（即 `scriptPath`），与 jar 部署目录无关。
  代码从不直接引用 rpm 路径，只把包名写进脚本的 `g_database_rpm_file=`，
  再以 `scriptPath` 为工作目录执行脚本 —— 裸文件名靠 CWD 解析。
  包名与 `X86_PACKAGE` / `ARM_PACKAGE` 必须逐字符一致。
- **类名拼写错误**：`PrepareSecnarioEnvironment`、`GBaseRandomReadSecnario`、
  `GBaseReadAndInsertSecnario` 里是 `Secnario` 而非 `Scenario`，搜索时注意。
- **`Mockdata.waitForVaildFiles` 会一直阻塞到数据文件齐备 —— 这是有意设计**：
  每 30s 跑一次 `find` 校验目录下是否已有 `mockdata.file.num` 个 >4080MB 的文件，
  不满足就继续等。造数据本身耗时很长，所以不设上限。
  注意方法内的 `timeOut`（6h）和 `countTimes` 是**未生效的残留变量**
  —— 判断写在 `while` 之外、只执行一次，别误以为 6 小时后会自动返回。
  它返回 `false` 只发生在校验命令本身出错时，不代表「还没造完」。
- **`mock.sh` 里每条命令都以 `&` 结尾**，脚本把 `mockdata.months` 个 java 进程拉起来就立即退出。
  所以 `runMockScript()` 返回 true 只说明「启动成功」，真正的产物校验靠 `waitForVaildFiles`。
- **`SceneExecutorNew` 与 `TestConfig` 字段重复**（前者少一个 `Connection`）。
  `UpdateConfProperties` 用前者取连接信息。改连接相关字段时两边都要动。
- 日志文件名（含 `iostat`/`dstat` 采集结果）直接写在 CWD，形如
  `Scenario1.copy.iostat.600.log`、`Scenario3_read_out_20250725_092415.log`。
