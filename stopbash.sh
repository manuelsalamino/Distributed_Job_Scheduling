stopbash () {
	for id in "$@"
	do 
		for pid in $( ps ax | grep "Executor.py $id" | awk '{print $1}'); do echo $pid; kill $pid; break; done
	done
}
