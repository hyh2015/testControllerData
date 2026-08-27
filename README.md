# TestControllerData

数据库性能压测的**调度控制器**。它本身不产生负载，而是按场景改写配置、拉起外部压测 jar、
在各阶段启停 `iostat`/`dstat` 采集 IO，最后查询记录表汇总指标。

支持的数据库类型：`ivory`（IvorySQL）、`highgo`（瀚高）、`vastdata`（Vastbase）、
`kingbase`（人大金仓）、`gbasedbt`（GBase 8s）、`pgdb`（原生 PostgreSQL，面向 PG 17）。

> 面向开发者的架构说明见 [CLAUDE.md](CLAUDE.md)。本文档面向压测执行人员。

---

## 一、环境准备

### 1.1 必装工具

| 工具 | 用途 | 缺失后果 |
|---|---|---|
| **JDK 8 或 11** | 运行主程序与各压测负载 | 无法运行 |
| **storcli** | 磁盘/RAID 健康预检 | **压测直接终止**，可关闭，见下方说明 |
| `iostat`（sysstat） | IO 指标采集 | 采集进程起不来 |
| `dstat` | 系统资源采集 | 同上 |
| `timeout`（coreutils） | 限时执行并发读程序 | 场景 3/5 无法限时 |
| `expect` | ivory 安装脚本自动化交互 | 仅安装 ivory 时需要 |

```bash
yum install -y sysstat dstat expect
```

> ⚠️ **默认情况下 storcli 是硬性前置条件。** 程序启动时执行磁盘预检，
> 若 storcli 未安装、路径不是 `/opt/MegaRAID/storcli/storcli64`、无执行权限或退出码非 0，
> 预检一律判定不通过，程序 `System.exit(1)` 终止。
> 这是有意设计：无法确认磁盘状态时，压测数据不具备参考价值。
>
> **非 MegaRAID 机器**（云主机、软 RAID 等）在 `allconf.properties` 中置
> `hardware.check.enabled=false` 即可跳过整个预检。跳过后磁盘坏盘或 Raid 降级
> 将无法被发现，启动日志会打出一条 WARN 提示。

### 1.2 磁盘容量

生成的测试数据默认约 **1.5 TB**（`mockdata.file.num=365`，每个文件 > 4080 MB），
必须放在**已挂载的数据盘**上，不能放系统盘。

`Mockdata.waitForVaildFiles` 会每 30 秒轮询一次，直到目录下出现
`mockdata.file.num` 个大于 4080 MB 的文件才继续，**不设超时上限**。
容量不足会一直等待，不会报错退出。

造数据阶段会按 `mockdata.months=12` **并发拉起 12 个 java 进程**，需评估内存与 IO 承载。

---

## 二、构建与部署

### 2.1 构建

`TestControllerData.jar` 是一个 **fat jar**：并发随机读程序（`tableMigration`）、
入库程序（`InsertIntoOracle`）以及全部 JDBC 驱动、日志库都已打进包内，
部署时不再需要平铺几十个 jar。

构建机上**只需要 JDK 8 或 11**，不必预装 Maven —— 项目自带 Maven Wrapper（`mvnw`），
首次运行会自动下载 Maven 3.8.5。

产物固定编译为 **Java 8 字节码**（pom 里 `source/target=8`），所以用 JDK 8 还是 11 构建都可以，
产出的 jar 在 JDK 8 和 11 上都能运行。用 JDK 11 构建时 javac 会提示一句
「未与 -source 8 一起设置引导类路径」，属正常告警，可忽略。

首次构建前执行一次依赖本地化（把 `../libDBB/` 下的 jar 灌进项目内的 Maven 文件仓库
`lib-repo/`，之后构建无需联网）：

```bash
./install-libs.sh
```

`libDBB` 不在默认位置时用 `LIB_DIR` 指定：

```bash
LIB_DIR=/path/to/libDBB ./install-libs.sh
```

然后打包：

```bash
./mvnw clean package
```

产物为 `target/TestControllerData.jar`（约 22 MB）。

> Windows 上用 `mvnw.cmd`。若已装好 Maven 也可直接用 `mvn`，两者等价。
> Linux 上若提示权限不足，先 `chmod +x mvnw install-libs.sh`。

### 2.2 部署

上传到服务器数据盘目录下，运行目录只需三个文件：

```
部署目录/
├── TestControllerData.jar          # 主程序（已含读、写两个压测程序与全部驱动）
├── mockdata.jar                    # 生成测试数据（自带依赖，独立交付）
└── allconf.properties              # 唯一需要手工配置的文件
```

> `mockdata.jar` 之所以不并入：它自身就是 8.7 MB 的自包含 fat jar，内置 logback，
> 与主程序的 log4j2 绑定冲突；且它本来就通过运行时生成的 `mock.sh` 独立调用。
> 不启用 `scene.mock.enabled` 时甚至不需要上传它。

**不需要**上传 `config.properties`、`l2o.properties`、`mock.sh` —— 这三个文件由程序
在运行时自动生成并整体覆盖，手工编辑不会生效。

---

## 三、配置 allconf.properties

这是**唯一**需要手工修改的配置文件。也可用 `-Dconf=/path/to/xxx.properties` 指定其它路径。

> ### ⚠️ 配置的键名与值必须使用纯 ASCII 字符
>
> **不要在配置值里写中文**（也不要写全角标点、带音标的字母等任何非 ASCII 字符），
> 尤其是 `data.path`、`partition.table_name`、`insert.tablename`
> 以及各数据库的 `user` / `password` / `database` / `url`。
>
> 原因是整条配置链路的解码方式不一致：控制器把 `allconf.properties` 的部分值
> 转写进 `l2o.properties` / `config.properties`，写出时用 UTF-8，
> 而真正消费这两个文件的随机读程序（`PropertyManager`）和
> 入库程序（`PropertiesUtil`）都使用 `Properties.load(InputStream)`
> ——该方法按 Java 规范**固定以 ISO-8859-1 解码**。非 ASCII 值会在这一步被曲解，
> 表现为找不到目录、连不上库或表名不匹配，且不一定有明确报错。
>
> **注释不受影响**（解析时会被跳过），但本文件的注释同样建议用英文书写：
> IDE 对 `.properties` 的默认编码通常也是 ISO-8859-1，中文注释会显示为乱码。
>
> 只要坚持全 ASCII，上述解码差异不会造成任何问题。

### 3.1 必改项

```properties
# 当前测试的数据库类型
db.type=ivory

# 对应类型的连接信息（键名前缀必须与 db.type 一致）
ivory.host=192.168.x.x          # 当前测试服务器 IP
ivory.port=5966
ivory.user=mycat
ivory.password=mycat
ivory.database=ivorysql

# 测试数据文件存放路径，必须在已挂载的数据盘下（约需 1.5TB）
data.path=/已挂载磁盘目录/TestControllerData/datafile

# 硬盘/Raid 预检开关，缺省 true；非 MegaRAID 机器置 false 跳过
hardware.check.enabled=true
```

> 新增数据库类型时，`{db.type}.driver` / `.url` / `.host` / `.port` / `.user` /
> `.password` / `.database` 这一整组键必须补齐，缺任意一个都会在建连接时抛 NPE。

### 3.2 安装 IvorySQL 时的额外配置

仅当 `db.type=ivory` 且 `is.install.ivory=true` 时生效。

```properties
is.install.ivory=true
scriptPath=/已挂载磁盘目录/ivoryTest      # setupivory.sh 与 rpm 包所在目录
mount.path=/已挂载磁盘目录/ivorydata      # ivory 数据目录，须在挂载盘下
X86_PACKAGE=IvorySQL-5.3-xxxx.x86_64.rpm
ARM_PACKAGE=IvorySQL-5.3-xxxx.aarch64.rpm
```

#### 测试前必须先准备好安装目录

**rpm 安装包要和 `setupivory.sh` 放在同一个目录**（即 `scriptPath`），
**不是**放在主程序 jar 的目录。两个目录相互独立：

```
/已挂载磁盘目录/ivoryTest/                          ← scriptPath
├── setupivory.sh                                  # 安装脚本，必须存在
└── IvorySQL-5.3-xxxx.x86_64.rpm                   # 与脚本同目录，按架构放对应包

/已挂载磁盘目录/TestControllerData/                  ← 主程序部署目录
├── TestControllerData.jar
├── mockdata.jar
└── allconf.properties
```

之所以是这个规则：程序**从不直接引用 rpm 文件路径**，它只把包名字符串写进脚本的
`g_database_rpm_file=` 变量（`DatabaseInstaller.updateSetupScript()`），
而安装命令执行时工作目录被设为 `scriptPath`（`builder.directory(new File(scriptPath))`），
所以脚本里的裸文件名是相对 `scriptPath` 解析的。

检查清单：

1. `setupivory.sh` 已放入 `scriptPath`，且当前用户有读写权限（程序会**改写**这个文件）
2. 对应架构的 rpm 已放入同一目录，用 `uname -m` 确认架构后放 x86_64 或 aarch64 的包
3. **rpm 文件名与 `X86_PACKAGE` / `ARM_PACKAGE` 的值逐字符一致** ——
   程序按 `uname -m` 选其一原样写进脚本，名字对不上脚本就找不到包
4. `mount.path` 指向的数据目录在挂载盘下，且为空或不存在
5. 已安装 `expect`（安装脚本的交互自动化依赖它）

要点：

- `scriptPath` 同时是 **`setupivory.sh` 所在目录**和**安装命令的工作目录**，
  与主程序 jar 放在哪里无关。该脚本必须存在，否则抛 `FileNotFoundException`。
- **安装失败会当场终止程序**（fail-fast），并抛出说明具体是哪一步不成立的异常：
  架构无法识别、包名未配置、`setupivory.sh` 改写失败、安装脚本未输出成功标记、
  或安装完成但状态检查（ivorysql 进程 / 端口 5966）未通过。
  不会带着没装成的库继续跑压测 —— 那样最终只会报「无法获取数据库连接」，
  报错点与真正的原因相距很远。
- 安装成功的标志是日志中出现「数据库：[ivorysql]安装成功，状态正常。」
- 判断「是否已安装」用的是 `rpm -qa | grep ivorysql`。只要装过任意版本的 ivorysql
  就会跳过安装 —— 要换版本必须先手工 `rpm -e` 卸载干净。
- 安装命令写死了 `-u mycat`，所以 `ivory.user` 应保持 `mycat`。
- **不要手工修改 `setupivory.sh` 里的包名**。程序按 `uname -m` 判定架构，
  自动用 `X86_PACKAGE` / `ARM_PACKAGE` 改写脚本中的 `g_database_rpm_file=` 行，
  并用 `mount.path` 改写 `g_database_data=` 行。手工改动会被覆盖。
- 当前架构对应的 PACKAGE 键若留空，会写入空包名，安装必然失败。
- ⚠️ **`ivory.port` 建议保持 5966**。安装完成后的状态检查把端口 5966 写死在
  `ss -lntp | grep 5966`（`DatabaseInstaller.java:80`），这与 ivory 安装脚本的默认端口一致，
  常规场景无需改动。若确需改用其它端口，JDBC 连接不受影响，但安装后的状态检查
  会误报「未检测到端口 5966 监听，数据库可能未启动」—— 此时需同步修改该处代码。
- `is.install.ivory=true` 但 `db.type` 不是 ivory 时，程序**直接抛 `IllegalStateException` 终止**。
  配置写错会当场停住，不会带着错误配置跑完整轮压测。

### 3.3 场景开关

```properties
scene.mock.enabled=true            # 生成测试数据
scene.createPartition.enabled=true # 仅创建分区表（不含索引）
scene1.enabled=true                # 批量入库（COPY STDIN）+ 创建 4 个分区索引
scene2.enabled=true                # distinct 计算
scene3.enabled=true                # 并发随机读
scene4.enabled=true                # 逐条入库
scene5.enabled=true                # 读写并发

# GBase 8s 专用场景
GBbase8s.read.enabled=false
GBbase8s.readAndinsert.enabled=false
```

要点：

- **`scene.createPartition.enabled` 只创建分区表，不创建索引。**
  4 个分区索引（usernum / imei / imsi / lai,ci）是在**场景 1** 中创建的，关掉场景 1 就没有索引。
- **场景之间有顺序依赖，不能任意单独执行**：
  - 场景 2 依赖场景 1 已灌入数据
  - 场景 3 依赖场景 2 产出的 `tb_usernum_list1`（会被改名为 `tb_usernum_list`），
    找不到会抛 `RuntimeException` 中断
  - 全部按顺序执行则无此问题
- **漏写某个开关等于开启**：读取逻辑 `DbManager.isEnabled(key)` 的缺省值是 `true`，
  关闭场景必须显式写 `=false`，不能靠删除该行。

### 3.4 影响单轮耗时的参数

`readme` 中的「其他配置参数默认」需注意，以下几项直接决定整体跑多久：

| 参数 | 含义 |
|---|---|
| `timeout.read.hour` | 场景 3 并发随机读时长 |
| `timeout.binfa.hour` | 场景 5 中读任务的时长上限（场景 5 实际由**写**任务完成来结束） |
| `insert.file.num` | 场景 4 每轮入库文件数，固定跑 3 轮 |
| `binfaInsert.file.num` | 场景 5 入库文件数 |
| `copy.file.num` / `copy.thread.num` | 场景 1 批量入库的文件数与并发数 |

---

## 四、运行

```bash
nohup java -jar TestControllerData.jar > nohup.out 2>&1 &
```

指定其它配置文件：

```bash
nohup java -Dconf=/path/to/allconf.properties -jar TestControllerData.jar > nohup.out 2>&1 &
```

程序**必须在部署目录下启动** —— 配置文件名、生成的 `mock.sh` 以及输出日志
都按裸文件名相对当前工作目录解析。读、写两个子程序同样从当前工作目录读取
`config.properties` / `l2o.properties`（它们忽略命令行传入的文件名，只认这两个固定名字）。

---

## 五、日志

| 位置 | 内容 |
|---|---|
| `nohup.out` | 控制台全量输出，可看到测试详细流程步骤 |
| `logs/controller.log` | 调度器自身的滚动日志（INFO 及以上） |
| `logs/tableMigration.log` | 并发随机读程序的滚动日志 |
| `logs/l2o.log` | 入库程序的滚动日志 |
| `logs/error-controller.log` / `error-reader.log` / `error-writer.log` | 三者各自的 ERROR 级别日志 |
| `Scenario*.log` | 各场景子进程的控制台输出，如 `Scenario3_read_out_20250725_092415.log` |
| `Scenario*.iostat.*.log` / `*.dstat.*.log` | 各阶段 IO 与资源采集结果 |

> 三个程序打在同一个 jar 里，无法再靠各自的同名 `log4j2.xml` 区分日志去向，
> 因此拆成三份配置：调度器用默认的 `log4j2.xml`，两个子进程由
> `JavaProcessExecutor` 启动时通过 `-Dlog4j.configurationFile` 分别指定
> `log4j2-reader.xml` 和 `log4j2-writer.xml`。
>
> error 日志按程序分文件，是因为场景 5 会让读、写两个子进程并发运行，
> 共用一个 `error.log` 时两个 JVM 的追加写会互相穿插。

---

## 六、常见问题

**安装 ivory 报 `/usr/bin/expect: No such file or directory`**

```bash
yum install -y expect
```

**程序启动即退出，日志显示预检不通过**

查看 `nohup.out` 中 `[硬盘预检]` 开头的记录：

- 提示「未找到可执行的 storcli」→ 安装 storcli，或确认路径为
  `/opt/MegaRAID/storcli/storcli64` 且有执行权限；
  非 MegaRAID 机器可置 `hardware.check.enabled=false` 跳过预检
- 提示「检测到硬盘硬件异常」→ 存在坏盘或 RAID 降级，需先修复硬件，
  详细状态见日志中的 VD / PD 报告

**造数据阶段长时间无进展**

程序在等待 `data.path` 下生成 `mockdata.file.num` 个大于 4080 MB 的文件，
每 30 秒检查一次且不会超时退出。先确认磁盘剩余容量是否足够（约需 1.5 TB），
再检查 `mock.sh` 拉起的 mockdata 进程是否正常。

**跑完某个场景后指标为空或报表不存在**

确认前置场景已执行。场景 3 需要场景 2 产出的号码池表，场景 2 需要场景 1 已灌入数据。
