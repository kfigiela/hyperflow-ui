package controllers

import (
	"bytes"
	"encoding/json"
	"github.com/revel/revel"
	"github.com/revel/revel/cache"
	"net/http"
	"strconv"
	"time"
)

const (
	defaultBaseUrl = "http://localhost:51404/apps"
	mediaType      = "application/json"
)

type Config struct {
	BaseURL string
}

type App struct {
	*revel.Controller
}

func (c App) Index() revel.Result {
	c.RenderArgs["molecules"] = strconv.FormatUint(uint64(1000), 10)
	c.RenderArgs["min_temperature"] = strconv.FormatFloat(float64(85.0), 'f', -1, 64)
	c.RenderArgs["max_temperature"] = strconv.FormatFloat(float64(90.0), 'f', -1, 64)
	c.RenderArgs["step_temperature"] = strconv.FormatFloat(float64(1.0), 'f', -1, 64)
	c.RenderArgs["simulation_end_time"] = strconv.FormatFloat(float64(5.5), 'f', -1, 64)
	return c.Render()
}

type WorkflowObject struct {
	Processes []ProcessesType `json:"processes"`
	Signals   []SignalsType   `json:"signals"`
	Ins       []string        `json:"ins"`
	Outs      []string        `json:"outs"`
}

type ProcessesType struct {
	Name     string     `json:"name"`
	Function string     `json:"function"`
	Type     string     `json:"type"`
	Config   ConfigType `json:"config"`
	Ins      []string   `json:"ins"`
	Outs     []string   `json:"outs"`
}

type SignalsType struct {
	Name string   `json:"name"`
	Data []string `json:"data,omitempty"`
}

type ConfigType struct {
	Executor ExecutorType `json:"executor"`
}

type ExecutorType struct {
	Executable string   `json:"executable"`
	Args       []string `json:"args"`
}

type Filenames struct {
	FilenameOutArchived string
	FilenameOutVideo    string
}

type Experiment struct {
	Stamp           string
	Molecules       string
	MinTemperature  string
	MaxTemperature  string
	StepTemperature string
	SimulationTime  string
	Status          string
	Artifacts       string
	Tgz             string
	Avi             string
	Workflow        WorkflowObject
}

func (c App) NewWorkflow(
	number_of_molecules uint,
	min_temperature float32,
	max_temperature float32,
	step_temperature float32,
	simulation_end_time float32,
	record_movie bool,
) revel.Result {

	// validate incoming form data
	c.Validation.Required(number_of_molecules).Message("Number of molecules is required")
	// c.Validation.Range(min_temperature, 0, 100).Message("Min temperature is required (in Celsius)")
	// c.Validation.Range(max_temperature, 0, 100).Message("Max temperature is required (in Celsius)")
	// c.Validation.Range(step_temperature, 0, 100).Message("Temperature step is required (in Celsius)")
	c.Validation.Required(simulation_end_time).Message("End time of simulation is required (in seconds)")

	// error pop-ups
	if c.Validation.HasErrors() {
		c.Validation.Keep()
		c.FlashParams()
		return c.Redirect(App.Index)
	}

	now := strconv.FormatInt(int64(time.Now().Unix()), 10)
	f := Filenames{
		FilenameOutArchived: "md-simulation-" + now + ".tgz",
		FilenameOutVideo:    "md-simulation-" + now + ".avi",
	}

	// generate workflow
	var workflowDescription WorkflowObject

	workflowDescription.Ins = []string{"start"}
	workflowDescription.Signals = []SignalsType{
		SignalsType{Name: "start", Data: []string{"start"}},
	}

	for temperature := min_temperature; temperature <= max_temperature; temperature += step_temperature {
		strTemp := strconv.FormatFloat(float64(temperature), 'f', -1, 64)
		process := ProcessesType{
			Name:     "run-simulation-" + strTemp,
			Function: "amqpCommand",
			Type:     "dataflow",
			Config: ConfigType{
				Executor: ExecutorType{
					Executable: "/MD_v4_MPI/run-cmd.sh",
					Args: []string{
						strconv.FormatUint(uint64(number_of_molecules), 10),
						strconv.FormatFloat(float64(simulation_end_time), 'f', -1, 64),
						strconv.FormatFloat(float64(temperature), 'f', -1, 64),
						"md-simulation-" + now + "-" + strTemp + ".tgz",
					},
				},
			},
			Ins:  []string{"start"},
			Outs: []string{"md-simulation-" + now + "-" + strTemp + ".tgz"},
		}
		workflowDescription.Processes = append(workflowDescription.Processes, process)
		workflowDescription.Outs = append(workflowDescription.Outs, "md-simulation-"+now+"-"+strTemp+".tgz")
		workflowDescription.Signals = append(workflowDescription.Signals, SignalsType{Name: "md-simulation-" + now + "-" + strTemp + ".tgz"})

		if record_movie == true {
			process := ProcessesType{
				Name:     "run-povray-" + strTemp,
				Function: "amqpCommand",
				Type:     "dataflow",
				Config: ConfigType{
					Executor: ExecutorType{
						Executable: "/MD_v4_MPI/make-movie.sh",
						Args: []string{
							"md-simulation-" + now + "-" + strTemp + ".tgz",
							"md-simulation-" + now + "-" + strTemp + ".avi",
						},
					},
				},
				Ins:  []string{"md-simulation-" + now + "-" + strTemp + ".tgz"},
				Outs: []string{"md-simulation-" + now + "-" + strTemp + ".avi"},
			}
			workflowDescription.Processes = append(workflowDescription.Processes, process)
			workflowDescription.Outs = append(workflowDescription.Outs, "md-simulation-"+now+"-"+strTemp+".avi")
			workflowDescription.Signals = append(workflowDescription.Signals, SignalsType{Name: "md-simulation-" + now + "-" + strTemp + ".avi"})

		}
	}

	// post workflow to HyperFlow
	b, err := json.MarshalIndent(workflowDescription, "", "  ")
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", defaultBaseUrl, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", mediaType)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	// status CREATED is expected
	statusURL := ""
	if resp.StatusCode != http.StatusCreated {
		panic(resp)
	} else {
		location, err := resp.Location()
		if err != nil {
			panic(err)
		}
		statusURL = location.String()
	}

	timestamp := time.Now()
	timestamp_str := timestamp.Format(time.Stamp)

	c.RenderArgs["filenames"] = f
	c.RenderArgs["simulation_end_time"] = simulation_end_time
	c.RenderArgs["molecules"] = number_of_molecules
	c.RenderArgs["timestamp"] = timestamp_str
	c.RenderArgs["min_temperature"] = min_temperature
	c.RenderArgs["max_temperature"] = max_temperature
	c.RenderArgs["step_temperature"] = step_temperature
	c.RenderArgs["statusURL"] = statusURL
	c.RenderArgs["workflow"] = workflowDescription

	return c.RenderTemplate("App/Index.html")
}

func (c App) CreateExperimentsTable() revel.Result {
	var newExperiment Experiment
	var experimentIds []string
	if err := cache.Get("experiment_ids", &experimentIds); err != nil {
		// no data, empty slice
	}
	if c.RenderArgs["timestamp"] != nil {
		min_temperature := c.RenderArgs["min_temperature"].(float32)
		max_temperature := c.RenderArgs["max_temperature"].(float32)
		step_temperature := c.RenderArgs["step_temperature"].(float32)
		molecules := c.RenderArgs["molecules"].(uint)
		simulation_end_time := c.RenderArgs["simulation_end_time"].(float32)

		newExperiment.Stamp = c.RenderArgs["timestamp"].(string)
		newExperiment.MinTemperature = strconv.FormatFloat(float64(min_temperature), 'f', -1, 64)
		newExperiment.MaxTemperature = strconv.FormatFloat(float64(max_temperature), 'f', -1, 64)
		newExperiment.StepTemperature = strconv.FormatFloat(float64(step_temperature), 'f', -1, 64)
		newExperiment.Molecules = strconv.FormatInt(int64(molecules), 10)
		newExperiment.SimulationTime = strconv.FormatFloat(float64(simulation_end_time), 'f', -1, 64)
		newExperiment.Status = "Running"
		filenames := c.RenderArgs["filenames"].(Filenames)
		newExperiment.Tgz = filenames.FilenameOutArchived
		newExperiment.Avi = filenames.FilenameOutVideo
		newExperiment.Workflow = c.RenderArgs["workflow"].(WorkflowObject)

		// cache.DEFAULT == one hour persistency
		if err := cache.Add(newExperiment.Stamp, newExperiment, cache.DEFAULT); err == nil {
			// new experiment
			experimentIds = append([]string{newExperiment.Stamp}, experimentIds...)
			cache.Set("experiment_ids", experimentIds, cache.DEFAULT)
		}
	}

	type ExperimentList []Experiment
	var myExperiments ExperimentList
	if g, err := cache.GetMulti(experimentIds...); err == nil {
		var oldExperiment Experiment
		for _, value := range experimentIds {
			if err := g.Get(value, &oldExperiment); err == nil {
				myExperiments = append(myExperiments, oldExperiment)
			}
		}
	}

	// provide S3 base url
	s3_region := revel.Config.StringDefault("aws.s3.region", "eu-central-1")
	s3_bucket := revel.Config.StringDefault("aws.s3.bucket", "paasage-bucket")
	s3_path := revel.Config.StringDefault("aws.s3.path", "results/")
	s3_base := "http://s3." + s3_region + ".amazonaws.com/" + s3_bucket + "/" + s3_path

	c.RenderArgs["s3_base"] = s3_base
	c.RenderArgs["myExperiments"] = myExperiments

	return c.RenderTemplate("App/Index.html")
}

type JsonMessage struct {
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (c App) UpdateExperiment(s3Resource string) revel.Result {
	if s3Resource == "" {
		data := JsonMessage{Error: "n/a"}
		return c.RenderJson(data)
	}

	response, err := http.Get(s3Resource)
	if err == nil && response.StatusCode == 200 {
		return c.RenderJson(JsonMessage{Status: "Finished"})
	} else {
		return c.RenderJson(JsonMessage{Status: "Running"})
	}
}
