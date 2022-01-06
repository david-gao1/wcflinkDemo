#!/bin/bash
echo "start run flink job"

MINIO_CHECKPOINT_PATH=""
RUNSQL=0001baisicCDC.sql
FLINK_ENV_PARALLELISM=1
#两个checkpoint的时间间隔，单位秒
CHECKPOINT_INTERVAL="30"
#checkpoint的生成路径，暂时使用flink-conf.yaml的配置
CHECKPOINT_PATH="s:"
#状态空闲过期时间，单位：小时
IDLE_STATE_RETENTION=12


RUNJARHOME=/data/app/big-event
RUNJARPATH=$RUNJARHOME/runjar/flink-cdc-big-event.jar
MAINCLASS=com.dodp.flink.wide.important.DodiImportEventTableWrapApp
SQLPATH=$RUNJARHOME/conf/flinkSQL/$RUNSQL
FLINKHOME=/data/app/flink-1.13.3

echo "run sql:" $RUNSQL " with path:" $SQLPATH

$FLINKHOME/bin/flink run \
  -s $MINIO_CHECKPOINT_PATH \
  -c $MAINCLASS $RUNJARPATH \
                $SQLPATH \
                $FLINK_ENV_PARALLELISM \
                $CHECKPOINT_INTERVAL \
                $CHECKPOINT_PATH \
                $IDLE_STATE_RETENTION \
