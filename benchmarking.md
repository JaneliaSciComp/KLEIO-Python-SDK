# Graphs:
Testing writing and checkout time progression.
#### Case 1: checkout every ~50..100 iteration
![Application](img/benchmarking/Picture1.png)
#### Case 2: Extreme case checkout a new branch everytime writing a new chunk
![Application](img/benchmarking/Picture2.png)

## Garbage collection test
Testing index chunk size (1,1,1) (MAX,MAX,MAX)

#### Checkout time: 
![Application](img/benchmarking/checkout%20gc%20all.png)

### Writing time:
![Application](img/benchmarking/writing%20gc%20all.png)
#### Both cases:
![Application](img/benchmarking/writing%20checkout%20gc%20100.png)

#### File size progression:
How file size progress per time in both cases
![Application](img/benchmarking/file%20size%20gc%20all.png)
![Application](img/benchmarking/file%20size%20gc%20100.png)


