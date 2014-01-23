# Cascading OpenCsv Scheme

At Tresata we sometimes need to parse csv files (or text files with other delimiters such as bar, tilde, tab, or semi-colon) that cannot be handled by Cascading's TextDelimited. The main issue seems to be csv files that are quoted and contain escaped quotes. It seems TextDelimited chose not to support this due to performance reasons. So we wrote OpenCsvScheme which uses the opencsv library (http://opencsv.sourceforge.net/) to be able to read those files.

Another issue that we wanted to deal with are csv files that are generally sound but have a few bad records. Nothing as frustrating as having a job that needs to process millions of records die because of a handful of bad lines! The OpenCsv scheme can run in non-strict mode where it will survive the failure to parse a line by opencsv and (the more typical situation) a line that parses successful but has the wrong number of items. In both cases in non-strict mode a warning will be logged about the offending line, a bad-line counter will be incremented, and the offending line will be skipped for processing. If you don't like this behavior then you can stick to strict mode where both these situations will cause an exception which will result in job failure.

Note that OpenCsv scheme is new and untested. We have no unit tests. So use at your own risk. Any bug-reports and bug-fixes are welcome.

OpenCsvScheme can be found here:
https://github.com/tresata/cascading-opencsv

Best,
Koert
Tresata
