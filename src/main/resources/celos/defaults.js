/*
 * Copyright 2015 Collective, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Set this to your Hadoop username
var CELOS_USER = "cloudera";
// Set this to your Oozie API endpoint
var CELOS_DEFAULT_OOZIE = "http://localhost:11000/oozie";
// Set this to your HDFS name node
var CELOS_DEFAULT_HDFS = "hdfs://quickstart.cloudera:8020";
// Set this to your Hadoop job tracker
var JOB_TRACKER = "quickstart.cloudera:8032 ";

// No changes beyond this point should be required.

var CELOS_DEFAULT_OOZIE_PROPERTIES = {
    "user.name": CELOS_USER,
    "jobTracker" : JOB_TRACKER,
    "nameNode" : CELOS_DEFAULT_HDFS
};