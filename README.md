# entity-resolution
Following is some code to perform entity resolution using Spark's GraphX API. 
I model the data in form of a graph, then connect the vertices based on match rules. 
Final merge them into common entities if the match rules are satisfied.

## Change log

### April 30th 2020
1. Support match rules as parameters
2. Support for partial matches (specify the column and error tolerance)
3. Added _Soundex_ and _Levenshtein_ string match algorithms

### May 4st 2020
1. Optimizations with the code for faster run time

### May 6th 2020
1. Added support for a mandatory field, it mandates that the specified
key should always match in the results

### May 12th 2020
Plan for the day
1. Setup neo4j in local
2. save the graph in neo4j
3. try to query neo4j

## Weight and Matching percentage
Attributes can be assigned weights
Formula for finding score for the weighted attributes

sum(weight*match_percentage)/sum(weights)

This value is compared with the threshold before going ahead and merging the entities


## Benchmark results
Graph connections formed 8997  
Graph connections formed 24  
Total Records before identity resolution 117993  
Total Records After identity resolution 104500  
Time taken: 4 minutes


## In Progress
Testing with bigger datasets 











