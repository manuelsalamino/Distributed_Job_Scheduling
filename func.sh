startEx () {
	while :
	do
		python Executor.py "$1" &
		LASTPID=$!
		echo "Start server $1 with pid: $LASTPID "
		sleep $[ ( $RANDOM % 20 ) + 1 ]s; kill $LASTPID
		echo "Server killed"
	done
}
