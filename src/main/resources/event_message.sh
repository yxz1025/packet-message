#!/bin/bash
now=$(date +%Y%m%d)
command='/opt/jdk1.8.0_51/bin/java -Xms50m -Xmx2048m -jar /alidata/www/packet-sendredpackets-0.0.1-SNAPSHOT.jar'
log_file_url="/${now}_sendredpackets.log"

start(){
if [ "$log_file_url" != "" ]; then
exec $command  > "$log_file_url" &
else
exec $command &
fi
}

stop(){
ps -ef | grep "$command" | awk '{print $2}' | while read pid
do
C_PID=$(ps --no-heading $pid | wc -l)
echo "当前PID=$pid"
if [ "$C_PID" == "1" ]; then
echo "PID=$pid 准备结束"
kill -9 $pid
echo "PID=$pid 已经结束"
else
echo "PID=$pid 不存在"
fi
done
}

case "$1" in
start)
start
;;
stop)
stop
;;
restart)
stop
start
;;
*)
printf 'Usage: %s {start|stop|restart}\n' "$prog"
exit 1
;;
esac