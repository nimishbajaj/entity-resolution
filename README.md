# entity-resolution
Following is some code to perform entity resolution using Spark's GraphX API. 
I model the data in form of a graph, then connect the vertices based on match rules. 
Final merge them into common entities if the match rules are satisfied.

## Change log

### April 30th 2020
1. Support match rules as parameters
2. Support for partial matches (specify the column and error tolerance)
3. Added _Soundex_ and _Levenshtein_ string match algorithms

### May 1st 2020
1. Optimizations with the code for faster run time


## In Progress
1. weightages
2. voting
3. hit level
4. identify user persona (marketing guys)
5. adobe with hubspot utk