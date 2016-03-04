celos.importDefaults("defaults");

var ROOT = "/user/" + CELOS_USER;

var RECEVIER_ID = "provider_receiver"
var RECEVIER_APPLICATION_PATH = ROOT  + "/deploy/receiver/workflow.xml";
var RECEVIER_INPUT_FACEBOOK_PATH = ROOT + "/input/facebook/${year}-${month}-${day}/${hour}00";
var RECEVIER_INPUT_NEXUS_PATH = ROOT + "/input/nexus/${year}-${month}-${day}/${hour}00";
var RECEVIER_OUTPUT_PATH = ROOT + "/receiver_output/${year}-${month}-${day}/${hour}00";

var AGGREGATOR_ID = "aggregator"
var AGGREGATOR_APPLICATION_PATH = ROOT + "/deploy/aggregator/workflow.xml"
var AGGREGATOR_INPUT_PATH = ROOT + "/receiver_output/${year}-${month}-${day}/${hour}00/*/*"
var AGGREGATOR_OUTPUT_PATH = ROOT + "/aggregator_output/${year}-${month}-${day}/${hour}00"

function defineReceiverWorkflow() {
    celos.defineWorkflow({
        "id": RECEVIER_ID,
        "schedule": celos.hourlySchedule(),
        "schedulingStrategy": celos.serialSchedulingStrategy(),
        "trigger": celos.hdfsCheckTrigger(RECEVIER_INPUT_NEXUS_PATH + "/_SUCCESS"),
        "externalService": celos.oozieExternalService({
            "oozie.wf.application.path": RECEVIER_APPLICATION_PATH,
            "oozie.use.system.libpath": "true",
            "inputFacebookPath": RECEVIER_INPUT_FACEBOOK_PATH,
            "inputNexusPath": RECEVIER_INPUT_NEXUS_PATH,
            "receiverOutputPath": RECEVIER_OUTPUT_PATH,
            "hive_defaults": HIVE_SITE_DEFAULTS
        })
    });
}

function defineAggregatorWorkflow() {
    celos.defineWorkflow({
        "id": AGGREGATOR_ID,
        "schedule": celos.hourlySchedule(),
        "schedulingStrategy": celos.serialSchedulingStrategy(),
        "trigger": celos.successTrigger(RECEVIER_ID),
        "externalService": celos.oozieExternalService({
            "oozie.wf.application.path": AGGREGATOR_APPLICATION_PATH,
            "oozie.use.system.libpath": "true",
            "aggregatorInputPath": AGGREGATOR_INPUT_PATH,
            "aggregatorOutputPath": AGGREGATOR_OUTPUT_PATH,
            "hive_defaults": HIVE_SITE_DEFAULTS
        })
    });
}

defineReceiverWorkflow();
defineAggregatorWorkflow();