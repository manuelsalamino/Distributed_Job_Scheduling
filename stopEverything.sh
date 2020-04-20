stopAll () {
	for pid in $( ps ax | grep Executor | awk '{print $1}'); do kill $pid; done
}
