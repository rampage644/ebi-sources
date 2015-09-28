# Description

Checking different RDBMS connections

# Features

 Goes through supplied _JSON_ document and extracts all possible _JDBC_ connection strings. For every connection finds
 all possible tables. For every table finds columns of some _DATE_ time to match against current date.
 
 Idea is that many tables contain timestamp column (such as _CREATED_AT_, _UPDATED_AT_, etc) and table data actuality
 could be checked. ыв