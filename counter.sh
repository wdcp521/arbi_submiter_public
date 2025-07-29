#!/bin/sh

file="logs/submiter.log"

# 确保文件存在
if [ ! -e "$file" ]; then
    echo "Error: File '$file' does not exist."
    exit 1
fi

echo "sdate\ttotal\tsuccess\tsuccessRate"
# 使用 awk 进行统计
awk '
{
    # 提取时间的小时部分
    split($1, datetime, "T")
    split(datetime[2], time, ":")
    day = datetime[1]
    hour = datetime[1] "T" time[1]  # 形成 "YYYY-MM-DDTHH" 格式

    # 统计提交txids的次数
    if ($0 ~ /submit txids/) {
        c[hour]++
        if (!/"", "", "", "", ""/) {
            d[hour]++
        }
    }
}
END {
    # 输出每小时的统计结果
    for (h in c) {
        successRate = (c[h] > 0) ? d[h] / c[h] : 0
        print h, c[h], d[h], successRate
    }
}
' "$file"|sort
