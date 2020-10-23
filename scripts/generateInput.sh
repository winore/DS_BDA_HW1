#!/bin/bash

LOGDATE=$( date +"%H%M%S_%d%m%y" )
FILENAME="TestData/TestData_$LOGDATE.txt"
USERAGENTS_SIZE=$( wc -l userAgents.txt | cut -d ' ' -f 1 )
SITES_SIZE=$( wc -l sites.txt | cut -d ' ' -f 1 )
LOG_SIZE=$((( RANDOM + 1 ) + 10))
#echo "Number of loglines - $LOG_SIZE"

if ( [ $LOG_SIZE -lt 10000 ] ) then
	LOG_SIZE=$(( LOG_SIZE + 10000 ))
fi
echo "Number of loglines - $LOG_SIZE"
> $FILENAME
OLD_LANG=$LANG
LANG="en_US"
echo -ne "\r0%"
for (( i = 0; i < $LOG_SIZE; i++))
do
	echo -ne "\r$(( ( i * 100 ) / LOG_SIZE ))%"	
	LOGLINE=""	
	IP=""
	IS_MALFORMED=$(( RANDOM % 2 ))
	MALFORMED_BYTE=4
	if ( [ $IS_MALFORMED -eq 1 ] ) then
		MALFORMED_TYPE=$(( RANDOM % 2 ))
		if ( [ $MALFORMED_TYPE -eq 0 ] ) then
			MALFORMED_BYTE=$(( RANDOM % 4 ))
		fi
	fi
		
	
	for (( j = 0; j < 4; j++)) do
		if ( [ $j -eq $MALFORMED_BYTE ] ) then
			IP_BYTE=$(( ( RANDOM % 744 ) + 256 ))
		else			
			IP_BYTE=$(( RANDOM % 256 ))
		fi
		if ( [ $j -eq 3 ] ) then		
			IP="$IP$IP_BYTE"
		else
			IP="$IP$IP_BYTE."
		fi
	done
	
	SECS=$(( ( ( RANDOM * RANDOM ) % 3000 ) + ( $( date +"%s" ) - 3000 )))

	DATE=$( date +"%d/%b/%Y:%H:%M:%S %z" --date="@$SECS" )
	SITE="http://$( head -$(( ( RANDOM % SITES_SIZE ) + 1 )) sites.txt | tail -1 )";
	
	CONTENT="/index.html"
	REQUEST="";
	if ( [ $(( RANDOM % 2 )) -eq 0 ] ) then
		REQUEST="GET $CONTENT HTTP/1.$(( RANDOM % 2 ))"
	else
		REQUEST="POST $CONTENT HTTP/1.$(( RANDOM % 2 ))"
	fi	

	RESPONSE=$(( RANDOM % 999 ))
	BYTES=$(( RANDOM ))
	USERAGENT=$( head -$(( ( RANDOM % USERAGENTS_SIZE ) + 1)) userAgents.txt | tail -1 )
	LOGLINE="$IP - - [$DATE] \"$REQUEST\" $RESPONSE $BYTES \"$SITE\" \"$USERAGENT\""	
	
	echo "$LOGLINE" >> $FILENAME
done
LANG=$OLD_LANG
echo -e "\r100%"
