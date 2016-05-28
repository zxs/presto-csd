#!/bin/bash

# Time marker for both stderr and stdout
date 1>&2

DEFAULT_PRESTO_HOME=/var/lib/presto
NODE_PROPERTIES_PATH=$DEFAULT_PRESTO_HOME/node.properties
JVM_DUMMY_CONFIG_PATH=$CONF_DIR/jvm.dummy.config
JVM_CONFIG_PATH=$CONF_DIR/etc/jvm.config
export JAVA_HOME=$CDH_PRESTO_JAVA_HOME
HIVE_CONF_PATH=$CONF_DIR/hive-conf
HBASE_CONF_PATH=$CONF_DIR/hbase-conf
PHOENIX_CONF_PATH=$CONF_DIR/phoenix-conf
IMPALA_CONF_PATH=$CONF_DIR/impala-conf

CMD=$1

function log {
  timestamp=$(date)
  echo "$timestamp: $1"	   #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

function generate_jvm_config {
  if [ -f $JVM_DUMMY_CONFIG_PATH ]; then
    cat $JVM_DUMMY_CONFIG_PATH | perl -e '$line = <STDIN>; chomp $line; $configs = substr($line, (length "jvm.config=")); for $value (split /\\n/, $configs) { print $value . "\n" }' > $JVM_CONFIG_PATH
  fi
}

function read_hadoop_site_property {
  local __file_name=$1
  local __prop_name=$2
  local __prop_value=`python - <<END
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
def getconfig(root, name):
  for existing_prop in root.getchildren():
    if existing_prop.find('name').text == name:
      return existing_prop.find('value').text

conf = ElementTree.parse("$__file_name").getroot()
prop_value = getconfig(root = conf, name = "$__prop_name")
print prop_value
END
`
echo $__prop_value
}

function substitute_hive_conn_tokens {
  [ -d "${HIVE_CONF_PATH}" ] || return ;
  local __hive_metastore_uri=$(read_hadoop_site_property "${HIVE_CONF_PATH}/hive-site.xml" "hive.metastore.uris" | cut -f1 -d,)
  sed -i -e "s#{{hive-conf}}#${HIVE_CONF_PATH}#g" \
    -e "s#{{hive-metastore-uri}}#${__hive_metastore_uri}#g" $CONF_DIR/etc/catalog/hive.properties

}

function substitute_phoenix_conn_tokens {
  [ -d "${HBASE_CONF_PATH}" ] || return ;

  local __zk_path=$(read_hadoop_site_property "${HBASE_CONF_PATH}/hbase-site.xml" "zookeeper.znode.parent")
  local __zk_quorum=$(read_hadoop_site_property "${HBASE_CONF_PATH}/hbase-site.xml" "hbase.zookeeper.quorum" )
  local __zk_port=$(read_hadoop_site_property "${HBASE_CONF_PATH}/hbase-site.xml" "hbase.zookeeper.property.clientPort")
#
#  local __zk_quorum_port=""
#  for h in `echo ${__zk_quorum} | tr "," "\n"`
#  do
#     __zk_quorum_port="${__zk_quorum_port},${h}:${__zk_port}"
#  done
#
  local __phoenix_connection_url="jdbc:phoenix:${__zk_quorum}:${__zk_port}:${__zk_path}"
  # :hbase@CHINANETCENTER.COM:/etc/security/keytab/hbszdx-hbase.keytab
  local __phoenix_connection_info="${HBASE_CONF_PATH}/core-site.xml,${HBASE_CONF_PATH}/hdfs-site.xml,${HBASE_CONF_PATH}/hbase-site.xml"
  if [ -f "${PHOENIX_CONF_PATH}/phoenix-site.xml" ]; then
    __phoenix_connection_info="${PHOENIX_CONF_PATH}/phoenix-site.xml,${__phoenix_connection_info}"
  fi


  sed -i -e "s#{{phoenix_connection_url}}#${__phoenix_connection_url}#g" \
    -e "s#{{phoenix_connection_info}}#${__phoenix_connection_info}#g" $CONF_DIR/etc/catalog/phoenix.properties
}

function substitute_impala_conn_tokens {
  [ -d "${IMPALA_CONF_PATH}" ] || (rm -f $CONF_DIR/etc/catalog/impala.properties; return;)
  local __hs2_port=$(grep "hs2_port" ${IMPALA_CONF_PATH}/impalad_flags | cut -f2 -d=)
  local __impalad=$(grep "hostname" ${IMPALA_CONF_PATH}/impalad_flags | cut -f2 -d=)
  local __impala_connection_url="jdbc:hive2://${__impalad}:${__hs2_port}/;"
  # principal=impala/hbszdx70@CHINANETCENTER.COM"
  local __impala_connection_info=""
  if [ -f "${IMPALA_CONF_PATH}/impala-site.xml" ]; then
    __impala_connection_info="${IMPALA_CONF_PATH}/impala-site.xml";
  fi
  sed -i -e "s#{{impala_connection_url}}#${__impala_connection_url}#g" \
    -e "s#{{impala_connection_info}}#${__impala_connection_info}#g" $CONF_DIR/etc/catalog/impala.properties
}


function link_files {
  cp -r $CDH_PRESTO_HOME/bin $CONF_DIR

  PRESTO_LIB=$CONF_DIR/lib
  if [ -L $PRESTO_LIB ]; then
    rm -rf $PRESTO_LIB
  fi
  ln -s $CDH_PRESTO_HOME/lib $PRESTO_LIB

  PRESTO_PLUGIN=$CONF_DIR/plugin
  if [ -L $PRESTO_PLUGIN ]; then
    rm -rf $PRESTO_PLUGIN
  fi
  ln -s $CDH_PRESTO_HOME/plugin $PRESTO_PLUGIN

  PRESTO_NODE_PROPERTIES=$CONF_DIR/etc/node.properties
  if [ -L $PRESTO_NODE_PROPERTIES ]; then
      rm -f $PRESTO_NODE_PROPERTIES
  fi
  ln -s $NODE_PROPERTIES_PATH $PRESTO_NODE_PROPERTIES
}

ARGS=()

case $CMD in

  (start_corrdinator)
    log "Startitng Presto Coordinator"
    link_files
    generate_jvm_config
    substitute_hive_conn_tokens
    substitute_phoenix_conn_tokens
    substitute_impala_conn_tokens
    ARGS=("--config")
    ARGS+=("$CONF_DIR/$2")
    ARGS+=("--data-dir")
    ARGS+=("$DEFAULT_PRESTO_HOME")
    ARGS+=("run")
    ;;

  (start_discovery)
    log "Startitng Presto Discovery"
    link_files
    generate_jvm_config
    substitute_hive_conn_tokens
    substitute_phoenix_conn_tokens
    substitute_impala_conn_tokens
    ARGS=("--config")
    ARGS+=("$CONF_DIR/$2")
    ARGS+=("--data-dir")
    ARGS+=("$DEFAULT_PRESTO_HOME")
    ARGS+=("run")
    ;;

  (start_worker)
    log "Startitng Presto Worker"
    link_files
    generate_jvm_config
    substitute_hive_conn_tokens
    substitute_phoenix_conn_tokens
    substitute_impala_conn_tokens
    ARGS=("--config")
    ARGS+=("$CONF_DIR/$2")
    ARGS+=("--data-dir")
    ARGS+=("$DEFAULT_PRESTO_HOME")
    ARGS+=("run")
    ;;

  (init_node_properties)
    if [ ! -f "$NODE_PROPERTIES_PATH" ]; then
      echo "node.environment=production" > $NODE_PROPERTIES_PATH
      echo "node.data-dir=/var/lib/presto" >> $NODE_PROPERTIES_PATH
      echo "node.id=`uuidgen`" >> $NODE_PROPERTIES_PATH
      log "create $NODE_PROPERTIES_PATH successfly"
    else
      log "$NODE_PROPERTIES_PATH is already created"
    fi
    exit 0

    ;;

  (*)
    log "Don't understand [$CMD]"
    ;;

esac

export PATH=$CDH_PRESTO_JAVA_HOME/bin:$PATH
cmd="$CONF_DIR/bin/launcher ${ARGS[@]}"
echo "Run [$cmd]"
exec $cmd
