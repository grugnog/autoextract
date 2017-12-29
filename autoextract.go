package autoextract

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
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
	Headers http.Header     `yaml:"headers"`
	BaseURL string          `yaml:"baseurl"`
	Items   map[string]Item `yaml:"items"`
	colly.Collector
	aLock                 *sync.RWMutex
	processEntryCallbacks []ProcessEntryCallback
}
type ProcessEntryCallback func(*colly.Response, string, json.Number, string, interface{})

// OnProcessEntry registers a function. Function will be executed on every entry.
func (s *Script) OnProcessEntry(f ProcessEntryCallback) {
	s.aLock.Lock()
	if s.processEntryCallbacks == nil {
		s.processEntryCallbacks = make([]ProcessEntryCallback, 0, 4)
	}
	s.processEntryCallbacks = append(s.processEntryCallbacks, f)
	s.aLock.Unlock()
}
func NewScript() *Script {
	script := &Script{}
	script.aLock = &sync.RWMutex{}
	return script
}

func NewScriptFromYAML(filename string) (*Script, error) {
	script := NewScript()
	yamlFile, err := ioutil.ReadFile("script.yaml")
	if err != nil {
		return script, errors.Wrapf(err, "could not read YAML file %s", filename)
	}
	err = yaml.Unmarshal(yamlFile, &script)
	if err != nil {
		return script, errors.Wrapf(err, "could not parse YAML file %s", filename)
	}
	return script, nil
}

func (s *Script) processEntries(r *colly.Response, tableName string, item Item, jsonParsed *gabs.Container) error {
	children, err := jsonParsed.S(item.Entries).Children()
	if err != nil {
		return err
	}
	for _, child := range children {
		log.Println(child)
		tasks, err := getTasks(item.Tasks)
		if err != nil {
			return err
		}
		data, ok := child.Data().(map[string]interface{})
		if !ok {
			return errors.New("top level of each entry must be an object")
		}
		data, err = executeTasks(tasks, data)
		if err != nil {
			return err
		}
		timestamp := time.Now().Format(time.RFC3339)
		id := child.S(item.EntryID).Data().(json.Number)
		if item.Timestamp != "" {
			timestamp = child.S(item.Timestamp).Data().(string)
		}
		for _, f := range s.processEntryCallbacks {
			f(r, tableName, id, timestamp, data)
		}
	}
	return nil
}

func (s *Script) Execute() error {
	c := colly.NewCollector()
	err := c.Limit(&colly.LimitRule{
		DomainRegexp: ".*",
		Parallelism:  5,
	})
	if err != nil {
		return errors.Wrap(err, "could not configure Colly limits")
	}
	c.CacheDir = "./cache"
	for tableName, item := range s.Items {
		c.OnResponse(func(r *colly.Response) {
			jsonDecoder := json.NewDecoder(bytes.NewReader(r.Body))
			jsonDecoder.UseNumber()
			jsonParsed, err := gabs.ParseJSONDecoder(jsonDecoder)
			if err != nil {
				// TODO: Figure out how to better handle errors here.
				log.Panicln(err)
				return
			}
			err = s.processEntries(r, tableName, item, jsonParsed)
			if err != nil {
				log.Panicln(err)
				return
			}
			for _, selector := range item.Follow {
				url, ok := jsonParsed.Path(selector).Data().(string)
				if ok {
					c.Request("GET", url, nil, nil, s.Headers)
				}
			}
		})

		c.Request("GET", s.BaseURL+item.Path, nil, nil, s.Headers)
	}
	return nil
}
