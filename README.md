## Aggregate By Key Notes
0 is the starting value
The 1st closure (lambda v1,v2: v1+1) works iteratively over all records with the same key but within a partition.
Say the 1st 3 records key, value pairs are:
*1\t5
*2\t6
*1\t7
For userId(key) 1, the 1st iteration takes the values 0(start value) and 5, and returns result of adding 0 and 1.
For the 2nd iteration for key 1, it takes values 1 (from previous calculation) and 7 from next record, and returns result of adding 1 to previous sum 1,
So key 1 now has a aggregated value of 2 and so on…
Note that the actual values 5 and 7 are not used directly but are just proxies to tell how many records there are for a key , each counting only once

The 2nd function passed (operator.add) adds up the values coming from the 1st lambda but it does this across different partitions
