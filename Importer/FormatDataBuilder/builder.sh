#!/bin/bash

mysqlhost=10.10.10.10
mysqluser=readonly
mysqlpassword=readonly
mysqldatabase=ccf_kg
mysqltable=write

isV=false

idField1=AuthorID
idField2=PaperID	# not used when assert(isV == true)

mask=$( 'ID' )

#################################################################


query="desc \`"$mysqltable"\`"

set -f
oldIFS=$IFS
IFS=$'\n'
results=( $(mysql --batch -h $mysqlhost -u$mysqluser -p$mysqlpassword -D$mysqldatabase <<< $query) )
len=${#results[@]}

if $isV
then
# V builder
query1="SELECT CONCAT( 'V ', label_, ' [', vtxid_, ']'"
query2=") FROM ( SELECT '$mysqltable' as label_, $idField1 as vtxid_"

for ((i=1;i<$len;i++)) ; do
  IFS=$'\t'
  read -r col1 col2 col3 <<< ${results[$i]}	# col1 : Filed   ;  col2 : Type
  if [[ $col1 == $idField1 ]]
  then
    continue
  fi
  flag=false
  for var in ${mask[@]} ; do
    if [[ $col1 == $var ]] ; then
      flag=true
      break
    fi
  done
  if $flag ; then
    continue
  fi
  query1=${query1}", ' [', prop${i}_, ',', type${i}_, ',\"""', value${i}_, '\"]'"
  if [[ ${col2:0:7} == $"varchar" ]] 	# string
  then
    query2=${query2}", '${col1}' as prop${i}_, 'string' as type${i}_, $col1 as value${i}_"
  else
    query2=${qeury2}", '${col1}' as prop${i}_, 'long' as type${i}_, $col1 as value${i}_"
  fi
  IFS=$'\n'
done

query=${query1}${query2}" FROM \`$mysqltable\` ) AS t"
(mysql -h $mysqlhost -u$mysqluser -p$mysqlpassword -D$mysqldatabase --default-character-set=utf8 --skip-column-names --raw <<< $query) > $mysqltable

else
# E builder
query1="SELECT CONCAT( 'E ', label_, ' [', Vlabel1_, '] [', Vid1_, '] [', Vlabel2_, '] [', Vid2_, ']'"
query2=") FROM ( SELECT '$mysqltable' as label_, '$idField1' as Vlabel1_, $idField1 as Vid1_, '$idField2' as Vlabel2_, $idField2 as Vid2_"

for ((i=1;i<$len;i++)) ; do
  IFS=$'\t'
  read -r col1 col2 col3 <<< ${results[$i]}	# col1 : Field   ;  col2 : Type
  if [[ $col1 == $idField1 || $col1 == $idField2 ]] ; then
    continue
  fi
  flag=false
  for var in ${mask[@]} ; do
    if [[ $col1 == $var ]] ; then
      flag=true
      break
    fi
  done
  if $flag ; then
    continue
  fi
  query1=${query1}", ' [', prop${i}_, ',', type${i}_, ',\"""', value${i}_, '\"]'"
  if [[ ${col2:0:7} == $"varchar" ]]	#string
  then
    query2=${query2}", '${col1}' as prop${i}_, 'string' as type${i}_, $col1 as value${i}_"
  else
    query2=${query2}", '${col1}' as prop${i}_, 'long' as type${i}_, $col1 as value${i}_"
  fi
  IFS=$'\n'
done

query=${query1}${query2}" FROM \`$mysqltable\` ) AS t"
(mysql -h $mysqlhost -u$mysqluser -p$mysqlpassword -D$mysqldatabase --default-character-set=utf8 --skip-column-names --raw <<< $query) > $mysqltable

fi

IFS=$oldIFS
