package autoextract

import (
	"bytes"
	"errors"
	"text/template"

	"github.com/Masterminds/sprig"
)

type Task struct {
	Config map[string]string
	Exec   TaskExecutor
}

type TaskExecutor func(map[string]string, map[string]interface{}) (map[string]interface{}, error)

func getTasks(configs []map[string]string) (tasks []Task, err error) {
	for _, data := range configs {
		taskType := ""
		config := make(map[string]string)
		for k, v := range data {
			if k == "type" {
				taskType = v
			} else {
				config[k] = v
			}
		}
		task := Task{Config: config}
		switch taskType {
		// Potentially replace this with a registry.
		case "template":
			task.Exec = templateTask
		default:
			err = errors.New("Could not identify task type")
			return
		}
		tasks = append(tasks, task)
	}
	return
}

func executeTasks(tasks []Task, input map[string]interface{}) (output map[string]interface{}, err error) {
	output = input
	for _, task := range tasks {
		output, err = task.Exec(task.Config, output)
		if err != nil {
			return
		}
	}
	return
}

func templateTask(config map[string]string, input map[string]interface{}) (output map[string]interface{}, err error) {
	output = input
	for k, v := range config {
		var result bytes.Buffer
		tmpl, err := template.New(k).Funcs(sprig.TxtFuncMap()).Parse(v)
		if err != nil {
			return nil, err
		}
		err = tmpl.Execute(&result, input)
		if err != nil {
			return nil, err
		}
		output[k] = result.String()
	}
	return output, nil
}
