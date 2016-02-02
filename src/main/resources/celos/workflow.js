celos.importDefaults("common");

var ROOT = "/user/" + CELOS_USER;
var APPLICATION_PATH = ROOT  + "/deploy/map-reduce/workflow.xml";
var INPUT_PATH = ROOT + "/input/${year}-${month}-${day}/${hour}00";

function defineWorkflow() {
    celos.defineWorkflow({
        "id": "parquetreader",
        "schedule": celos.hourlySchedule(),
        "schedulingStrategy": celos.serialSchedulingStrategy(),
        "trigger": celos.hdfsCheckTrigger(INPUT_PATH + "/_SUCCESS"),
        "externalService": celos.oozieExternalService({
            // These properties are passed to the Oozie job
            "oozie.wf.application.path": APPLICATION_PATH,
            "inputPath": INPUT_PATH
        })
    });
}

defineWorkflow();