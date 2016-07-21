
[success]: https://github.com/seanharr11/etlalchemy/blob/performance/img/green_check.png "Success"
[failure]: https://github.com/seanharr11/etlalchemy/blob/performance/img/red_x.png "Failure"

# Source -> Target: Performance/Timing Matrix
_Sources in first column, destinations in first row (DB Size: 4 million rows, 150MB)_

|     | MySQL | Postgresql | MSSQL | Oracle | SQLite |
| :--- | :--- | :--- | :--- | :---- | :--- |
| **MySQL** |4m:38s|4m:31s|61m:27s|63m:16s|2m:18s|
| **Postgresql** |5m:9s|4m:24s|58m:9s|61m:29s|3m:11s|
| **MSSQL** |5m:58s|5m:26s|57m:38s|60m:8s|5m:17s|
| **Oracle** |40m:14s|39m:25s|82m:26s| ??? |4m:0s|
| **SQLite** |4m:51s|4m:51s|67m:29s|64m:22s|2m:11s|
