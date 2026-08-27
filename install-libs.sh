#!/bin/bash
#
# 把依赖 jar 灌入项目内的 Maven 文件仓库 lib-repo/，供 mvn package 使用。
#
# 为什么需要这一步：
#   国产数据库驱动（Vastbase / HGDB / Kingbase / GBase / H2）和两个程序包
#   （tableMigration / InsertIntoOracle）都不在 Maven 中央仓库里，必须先本地化。
#   顺带把中央仓库能下到的那些也一并灌入，让构建完全离线、结果可复现。
#
# 装到项目内的 lib-repo/ 而不是 ~/.m2，是为了不覆盖机器上其他项目在用的同名构件。
#
# 用法：
#   ./install-libs.sh              # 默认从 ../libDBB 取 jar
#   LIB_DIR=/path/to/libDBB ./install-libs.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${LIB_DIR:-$SCRIPT_DIR/../libDBB}"
REPO_DIR="$SCRIPT_DIR/lib-repo"

# 默认用项目自带的 Maven Wrapper，机器上只要有 JDK 就能跑（首次会自动下载 Maven）。
# 想用系统已装的 Maven 就 MVN=mvn ./install-libs.sh
MVN="${MVN:-$SCRIPT_DIR/mvnw}"

if ! command -v "$MVN" >/dev/null 2>&1 && [ ! -x "$MVN" ]; then
    echo "错误：找不到可用的 Maven：$MVN" >&2
    echo "请确认 mvnw 存在且有执行权限（chmod +x mvnw），或用 MVN=/path/to/mvn 指定。" >&2
    exit 1
fi

if [ ! -d "$LIB_DIR" ]; then
    echo "错误：依赖目录不存在：$LIB_DIR" >&2
    echo "请用 LIB_DIR=<libDBB 的实际路径> 重新执行。" >&2
    exit 1
fi

echo "依赖来源：$LIB_DIR"
echo "目标仓库：$REPO_DIR"
echo

# 格式：jar文件名|groupId|artifactId|version
# 前 13 个用中央仓库的真实坐标，后 7 个是无公开坐标的私有件，归到 local.dbbench 下。
# mockdata.jar 不在此列 —— 它是自包含 fat jar（内置 logback，与主程序 log4j2 冲突），
# 保持独立交付，由 mock.sh 单独调用。
ARTIFACTS="
postgresql-42.7.5.jar|org.postgresql|postgresql|42.7.5
ojdbc7-12.1.0.2.0.jar|cn.easyproject|ojdbc7|12.1.0.2.0
mysql-connector-java-5.1.45.jar|mysql|mysql-connector-java|5.1.45
log4j-api-2.11.0.jar|org.apache.logging.log4j|log4j-api|2.11.0
log4j-core-2.11.0.jar|org.apache.logging.log4j|log4j-core|2.11.0
log4j-slf4j-impl-2.11.0.jar|org.apache.logging.log4j|log4j-slf4j-impl|2.11.0
log4j-1.2-api-2.11.0.jar|org.apache.logging.log4j|log4j-1.2-api|2.11.0
slf4j-api-1.7.10.jar|org.slf4j|slf4j-api|1.7.10
disruptor-3.3.4.jar|com.lmax|disruptor|3.3.4
druid-1.1.10.jar|com.alibaba|druid|1.1.10
guava-22.0.jar|com.google.guava|guava|22.0
commons-io-2.4.jar|commons-io|commons-io|2.4
commons-lang3-3.3.2.jar|org.apache.commons|commons-lang3|3.3.2
VastbaseG100_jdbc_2.9p_2023120616.jar|local.dbbench|vastbase-jdbc|2.9p
hgjdbc-6.2.4.build11.jar|local.dbbench|hgjdbc|6.2.4.build11
kingbase8-8.6.0.jar|local.dbbench|kingbase8|8.6.0
gbasedbtjdbc_3.6.5_NR_P20250826_86e700.jar|local.dbbench|gbasedbtjdbc|3.6.5
h2_driver.jar|local.dbbench|h2-driver|1.0
tableMigration.jar|local.dbbench|tableMigration|1.0
InsertIntoOracle.jar|local.dbbench|InsertIntoOracle|1.0
"

missing=0
for line in $ARTIFACTS; do
    jar="${line%%|*}"
    if [ ! -f "$LIB_DIR/$jar" ]; then
        echo "缺失：$LIB_DIR/$jar" >&2
        missing=$((missing + 1))
    fi
done

if [ "$missing" -gt 0 ]; then
    echo >&2
    echo "错误：$missing 个 jar 找不到，终止。" >&2
    exit 1
fi

count=0
for line in $ARTIFACTS; do
    IFS='|' read -r jar gid aid ver <<< "$line"
    count=$((count + 1))
    printf '[%2d/20] %s -> %s:%s:%s\n' "$count" "$jar" "$gid" "$aid" "$ver"
    "$MVN" -q org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file \
        -Dfile="$LIB_DIR/$jar" \
        -DgroupId="$gid" \
        -DartifactId="$aid" \
        -Dversion="$ver" \
        -Dpackaging=jar \
        -DcreateChecksum=true \
        -DlocalRepositoryPath="$REPO_DIR"
done

echo
echo "完成：$count 个依赖已装入 $REPO_DIR"
echo "现在可以执行：./mvnw clean package"
