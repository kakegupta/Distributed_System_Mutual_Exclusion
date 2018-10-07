# Distributed system (Mutual Exclusion Algorithms Implemented)

## Instructions to run the code
1. Compile the code using java 8 compiler and build a jar
2. To run the code put a "dsConfig" file in the same directory as the executable and run the following command
###### For coordinator process
	java Dproc -c

###### For registering processes
	java Dproc

### Sample dsConfig file
	COORDINATOR dc01
	NUMBER OF PROCESSES 5
	T1 50
	T2 200
	T3 500
	NEIGHBOR LIST
	1 2 3 4 5
	2 1 3 4 5
	3 1 2 4 5
	4 1 2 3 5
	5 1 2 3 4


## Algorithms Implemented
### Raymond's Algorithms
### Suzuki-Kasami's Algorithm

<object data="https://github.com/kakegupta/Distributed_System_Mutual_Exclusion/blob/master/Comparison_Study.pdf" type="application/pdf" width="700px" height="700px">
    <embed src="https://github.com/kakegupta/Distributed_System_Mutual_Exclusion/blob/master/Comparison_Study.pdf">
        <p>This browser does not support PDFs. Please download the PDF to view it: <a href="https://github.com/kakegupta/Distributed_System_Mutual_Exclusion/blob/master/Comparison_Study.pdf">Download Comparison Study</a>.</p>
    </embed>
</object>
