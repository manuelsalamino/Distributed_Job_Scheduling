# Distributed-Job-Scheduling
## Goal
Implement an infrastructure to manage jobs submitted to a cluster of Executors. Each client may submit a job to any of the executors receiving a job id as a return value. Through such job id, clients may check (contacting the same executor they submitted the job to) if the job has been executed and may retrieve back the results produced by the job.
Executors communicate and coordinate among themselves in order to share load such that at each time every Executor is running the same number of jobs (or a number as close as possible to that). Assume links are reliable but processes (i.e., Executors) may fail (and resume back, re-joining the system immediately after).
Use stable storage to cope with failures of Executors.

## Our choices
We implement the infrastructure in Python, using Python Socket to have Client-Executor and Executor-Executor communications.
We have decided to deploy the cluster of Executor as a **Ring Structure**, in order to **reduce message exchange**. The Token, passing through the Executors, manage the *load balancing* by updating an internal dataframe which has a report about the number of jobs (executing and waiting) of each Executor. Consulting this dataframe the Token is able to manage load balancing by telling to an Executor if it has to send jobs other Executors, and eventually to which one. If job(s) forward is needed, the two Executors establish a connection usign Pyhthon Socket, the recipient add the received job(s) to its waiting list (or directly executes the first one) and only when the result of a job is  computed it is sent to the sender. In this way we have, again, message exchange reduction because the sender never asks for the forwarded job status.

To manage **fault tolerance** we use **at least once** semantic: *messages may be duplicated, but not lost*. In fact every time there is a connection (Client-Executor or Executor-Executor) the sender always wait for an ACK from the receiver; we set a *timeout timer* and if the ACK if not received the sender send again the message.


## Bash Script
In order to simulate *failures* we create a bash script (each machine will execute one) in which two Executors and a Client are created. After a random time the bash script simulate the failure of an Executor in order to test the fault tolerance (test if the state is store correctly and then restored, and if after the re-joining of Executor everything works well).


## Demo
The Demo version that you can find executing the above code is composed by two Machines, each one of them must execute a bash script, and so each machine has two Executors and one Client.
The messages recipients are set by default in order to show that the connection between components of the same, or different, machine works in the right way.
