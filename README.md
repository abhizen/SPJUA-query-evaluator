# SPJUA-query-evaluator
This project implements SQL query evaluator with support for Select, Project, Join, Bag Union and Aggregate operation. Also supports specialized join algorithms and certain query optimization techniques that optimize query run time.

Query evaluator parses the query and creates relational algebra(RA) tree. Multiple optimization techniques are applied to the RA tree. Further iterator iterates through the RA tree and calls operator corresponding to each node of RA tree.

