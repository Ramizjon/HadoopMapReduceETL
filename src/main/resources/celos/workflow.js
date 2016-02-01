celos.importDefaults("common");

var ROOT = "/user/" + CELOS_USER;
var APPLICATION_PATH = ROOT  + "/deploy/map-reduce/workflow.xml";
var INPUT_PATH = ROOT + "/input/${year}-${month}-${day}/${hour}00";
var OUTPUT_PATH = ROOT + "/output/${year}-${month}-${day}/${hour}00";

function defineWorkflow() {
    celos.defineWorkflow({
        "id": "parquetreader",
        "schedule": celos.hourlySchedule(),
        "schedulingStrategy": celos.serialSchedulingStrategy(),
        "trigger": celos.hdfsCheckTrigger(inputPath + "/_SUCCESS"),
        "externalService": celos.oozieExternalService({
            // These properties are passed to the Oozie job
            "oozie.wf.application.path": APPLICATION_PATH,
            "inputPath": INPUT_PATH,
            "outputPath": OUTPUT_PATH
        })
    });
}

defineWorkflow();
