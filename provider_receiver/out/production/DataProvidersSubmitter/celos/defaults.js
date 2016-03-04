
var CELOS_USER = "cloudera";
var CELOS_DEFAULT_OOZIE = "http://localhost:11000/oozie";
var CELOS_DEFAULT_HDFS = "hdfs://quickstart.cloudera:8020";
var JOB_TRACKER = "quickstart.cloudera:8032 ";
var HIVE_SITE_DEFAULTS = "/user/cloudera/hive_config/hive-site.xml"

var CELOS_DEFAULT_OOZIE_PROPERTIES = {
    "user.name": CELOS_USER,
    "jobTracker" : JOB_TRACKER,
    "nameNode" : CELOS_DEFAULT_HDFS
};
