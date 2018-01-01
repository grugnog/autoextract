package autoextract

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/gocolly/colly"
	_ "github.com/markbates/goth"
	"github.com/pkg/errors"
	_ "github.com/qntfy/kazaam"
	"gopkg.in/yaml.v2"
)

type Item struct {
	Path      string              `yaml:"path"`
	Entries   string              `yaml:"entries"`
	EntryID   string              `yaml:"id"`
	Timestamp string              `yaml:"timestamp"`
	Follow    []string            `yaml:"follow"`
	Tasks     []map[string]string `yaml:"tasks"`
}

type Script struct {
	Sources map[string]Source `yaml:"sources"`
}

type Source struct {
	Headers http.Header     `yaml:"headers"`
	BaseURL string          `yaml:"baseurl"`
	Items   map[string]Item `yaml:"items"`
	colly.Collector
	processEntryCallbacks []ProcessEntryCallback
	processErrorCallbacks []ProcessErrorCallback
	pLock                 *sync.RWMutex
}
type ProcessEntryCallback func(*colly.Response, string, json.Number, string, interface{}) error
type ProcessErrorCallback func(*colly.Response, string, json.Number, string, interface{}, error)

// OnProcessEntry registers a function for  source. Function will be executed on every entry.
func (source *Source) OnProcessEntry(f ProcessEntryCallback) {
	source.pLock.Lock()
	if source.processEntryCallbacks == nil {
		source.processEntryCallbacks = make([]ProcessEntryCallback, 0, 4)
	}
	source.processEntryCallbacks = append(source.processEntryCallbacks, f)
	source.pLock.Unlock()
}

// OnProcessError registers a function. Function will be executed on error
// during response parsing or processing.
func (source *Source) OnProcessError(f ProcessErrorCallback) {
	source.pLock.Lock()
	if source.processErrorCallbacks == nil {
		source.processErrorCallbacks = make([]ProcessErrorCallback, 0, 4)
	}
	source.processErrorCallbacks = append(source.processErrorCallbacks, f)
	source.pLock.Unlock()
}

func NewScript() *Script {
	script := &Script{}
	return script
}

func NewSource() *Source {
	source := &Source{}
	source.Init()
	source.pLock = &sync.RWMutex{}
	return source
}

func NewScriptFromYAML(filename string) (*Script, error) {
	s := NewScript()
	yamlFile, err := ioutil.ReadFile("script.yaml")
	if err != nil {
		return s, errors.Wrapf(err, "could not read YAML file %s", filename)
	}
	err = yaml.Unmarshal(yamlFile, &s)
	if err != nil {
		return s, errors.Wrapf(err, "could not parse YAML file %s", filename)
	}
	for k, source := range s.Sources {
		source.Init()
		source.pLock = &sync.RWMutex{}
		s.Sources[k] = source
	}
	return s, nil
}

func (source *Source) processEntries(r *colly.Response, tableName string, item Item, jsonParsed *gabs.Container) {
	children, err := jsonParsed.S(item.Entries).Children()
	if err != nil {
		source.processErrors(r, tableName, "", "", jsonParsed.Data(), err)
	}
	for _, child := range children {
		tasks, err := getTasks(item.Tasks)
		if err != nil {
			source.processErrors(r, tableName, "", "", jsonParsed.Data(), err)
		}
		data, ok := child.Data().(map[string]interface{})
		if !ok {
			err = errors.New("top level of each entry must be an object")
			source.processErrors(r, tableName, "", "", data, err)
		}
		data, err = executeTasks(tasks, data)
		if err != nil {
			source.processErrors(r, tableName, "", "", data, err)
		}
		timestamp := time.Now().Format(time.RFC3339)
		id := child.S(item.EntryID).Data().(json.Number)
		if item.Timestamp != "" {
			timestamp = child.S(item.Timestamp).Data().(string)
		}
		source.pLock.RLock()
		for _, f := range source.processEntryCallbacks {
			err = f(r, tableName, id, timestamp, data)
			if err != nil {
				source.processErrors(r, tableName, id, timestamp, data, err)
			}
		}
		source.pLock.RUnlock()
	}
}

func (source *Source) processErrors(r *colly.Response, tableName string, id json.Number, timestamp string, data interface{}, err error) {
	source.pLock.RLock()
	for _, f := range source.processErrorCallbacks {
		f(r, tableName, id, timestamp, data, err)
	}
	source.pLock.RUnlock()
}

func (s *Script) Execute() error {
	for _, source := range s.Sources {
		err := source.Limit(&colly.LimitRule{
			DomainRegexp: ".*",
			Parallelism:  5,
		})
		if err != nil {
			return errors.Wrap(err, "could not configure Colly limits")
		}
		source.SetRequestTimeout(time.Second * 5)
		source.CacheDir = "./cache"
		for tableName, item := range source.Items {
			source.OnResponse(func(r *colly.Response) {
				jsonDecoder := json.NewDecoder(bytes.NewReader(r.Body))
				jsonDecoder.UseNumber()
				jsonParsed, err := gabs.ParseJSONDecoder(jsonDecoder)
				if err != nil {
					source.processErrors(r, tableName, "", "", jsonParsed.Data(), err)
				}
				source.processEntries(r, tableName, item, jsonParsed)
				for _, selector := range item.Follow {
					url, ok := jsonParsed.Path(selector).Data().(string)
					if ok {
						source.Request("GET", url, nil, nil, source.Headers)
					}
				}
			})
			source.Request("GET", source.BaseURL+item.Path, nil, nil, source.Headers)
		}
	}
	return nil
}
