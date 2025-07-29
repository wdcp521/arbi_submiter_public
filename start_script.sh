#!/bin/bash
#

# 脚本目录
script_path=$(dirname "$(readlink -f "$0")")

# 判断pids目录是否存在，不存在则创建
if [ ! -d "pids" ]; then
  mkdir pids
fi

# 读取命令行传入的参数
command=$1

config_dir='config'
logs_dir='logs'

if [ ! -d "$logs_dir" ]; then
  mkdir $logs_dir
fi

name='arbi_submiter'

# 启动或停止进程
case $command in
  start)
    # 启动逻辑，这里是示例，你可以根据实际需求修改
    echo "Starting $name..."
    # 启动本地进行
    if pgrep $name > /dev/null
    then
      echo "$name is running"
    else
      ./$name > logs/$name.debug.log 2>&1 &
      echo $! > "pids/$name.pid"
      echo "$name started..."
    fi
  ;;
  stop)
    # 停止逻辑，这里是示例，你可以根据实际需求修改
    echo "Stopping $name..."
    if [ -f "pids/$name.pid" ]; then
      kill $(cat "pids/$name.pid")
      rm "pids/$name.pid"
      echo "$name stopped..."
    fi
    sleep 1
    
    if pgrep $name > /dev/null
    then
      echo "Running process found for $name. kill -9"
      pgrep $name|awk '{print $2}'|xargs kill -9
    fi
  ;;
  restart)
    ./$0 stop $instance_num
    ./$0 start $instance_num
  ;;
  *)
    echo "Invalid command. Usage: ./start_script.sh (start|stop|restart)"
    exit 1
    ;;
esac

exit 0
