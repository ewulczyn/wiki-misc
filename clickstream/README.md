This directory contains code used to generate the [Wikipedia Clickstream](http://datahub.io/dataset/wikipedia-clickstream) data sets.

The raw request WMF logs are stored for 1 month in Hive. I run a daily [oozie job](https://github.com/ewulczyn/wmf/tree/master/clickstream/oozie)
that counts `(referer, request)' pairs. At the end of the month, I generate a monthly aggregate and join the data with the production 
page, redirect and pagelinks tables using this [script](https://github.com/ewulczyn/wmf/blob/master/clickstream/publish/generate_monthly_agg.sql).
