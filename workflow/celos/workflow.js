celos.importDefaults("defaults");

var ROOT = "/user/" + CELOS_USER;

var UNIFIER_ID = "provider_unifier"
var UNIFIER_APPLICATION_PATH = ROOT  + "/deploy/unifier/workflow.xml";
var UNIFIER_INPUT_FACEBOOK_PATH = ROOT + "/input/facebook/${year}-${month}-${day}/${hour}00";
var UNIFIER_INPUT_NEXUS_PATH = ROOT + "/input/nexus/${year}-${month}-${day}/${hour}00";
var UNIFIER_OUTPUT_PATH = ROOT + "/unifier_output/${year}-${month}-${day}/${hour}00";

var AGGREGATOR_ID = "aggregator"
var AGGREGATOR_APPLICATION_PATH = ROOT + "/deploy/aggregator/workflow.xml"
var AGGREGATOR_INPUT_PATH = ROOT + "/unifier_output/${year}-${month}-${day}/${hour}00/*/*"
var AGGREGATOR_OUTPUT_PATH = ROOT + "/aggregator_output/${year}-${month}-${day}/${hour}00"

function defineReceiverWorkflow() {
    celos.defineWorkflow({
        "id": UNIFIER_ID,
        "schedule": celos.hourlySchedule(),
        "schedulingStrategy": celos.serialSchedulingStrategy(),
        "trigger": celos.hdfsCheckTrigger(UNIFIER_INPUT_NEXUS_PATH + "/_SUCCESS"),
        "externalService": celos.oozieExternalService({
            "oozie.wf.application.path": UNIFIER_APPLICATION_PATH,
            "oozie.use.system.libpath": "true",
            "inputFacebookPath": UNIFIER_INPUT_FACEBOOK_PATH,
            "inputNexusPath": UNIFIER_INPUT_NEXUS_PATH,
            "unifierOutputPath": UNIFIER_OUTPUT_PATH,
            "hive_defaults": HIVE_SITE_DEFAULTS
        })
    });
}

function defineAggregatorWorkflow() {
    celos.defineWorkflow({
        "id": AGGREGATOR_ID,
        "schedule": celos.hourlySchedule(),
        "schedulingStrategy": celos.serialSchedulingStrategy(),
        "trigger": celos.successTrigger(UNIFIER_ID),
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