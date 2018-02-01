package autoextract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
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
	Path            string              `yaml:"path"`
	Entries         string              `yaml:"entries"`
	EntryID         string              `yaml:"id"`
	Timestamp       string              `yaml:"timestamp"`
	TimestampFormat string              `yaml:"timestamp_format"`
	Update          UpdateConfig        `yaml:"update"`
	Follow          []string            `yaml:"follow"`
	Tasks           []map[string]string `yaml:"tasks"`
	watermark       time.Time
	watermarkLock   *sync.RWMutex
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

type UpdateConfig struct {
	FullFrequency        string `yaml:"full_frequency"`
	IncrementalFrequency string `yaml:"incremental_frequency"`
	IncrementalKey       string `yaml:"incremental_key"`
	IncrementalValue     string `yaml:"incremental_value"`
	IncrementalFormat    string `yaml:"incremental_format"`
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

func (source *Source) processEntries(r *colly.Response, tableName string, item *Item, jsonParsed *gabs.Container) {
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
			// If a timestamp is configured, parse it and update the watermark if needed.
			timevalue, err := time.Parse(item.TimestampFormat, child.S(item.Timestamp).Data().(string))
			if err != nil {
				source.processErrors(r, tableName, "", "", data, err)
			}
			timestamp = timevalue.Format(time.RFC3339)
			item.watermarkLock.Lock()
			if timevalue.After(item.watermark) {
				item.watermark = timevalue
			}
			item.watermarkLock.Unlock()
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
	errs := make(chan error, 1)
	for _, source := range s.Sources {
		baseURL, err := url.Parse(source.BaseURL)
		if err != nil {
			return errors.Wrap(err, "could not parse base URL")
		}
		err = source.Limit(&colly.LimitRule{
			DomainRegexp: ".*",
			Parallelism:  5,
		})
		if err != nil {
			return errors.Wrap(err, "could not configure Colly limits")
		}
		source.SetRequestTimeout(time.Second * 5)
		source.AllowURLRevisit = true
		for tableName, item := range source.Items {
			item.watermarkLock = &sync.RWMutex{}
			// TODO: Initialize items from source level defaults.
			source.OnResponse(func(r *colly.Response) {
				jsonDecoder := json.NewDecoder(bytes.NewReader(r.Body))
				jsonDecoder.UseNumber()
				jsonParsed, err := gabs.ParseJSONDecoder(jsonDecoder)
				if err != nil {
					source.processErrors(r, tableName, "", "", jsonParsed.Data(), err)
				}
				source.processEntries(r, tableName, &item, jsonParsed)
				ctx := colly.NewContext()
				ctx.Put("update_source", r.Ctx.Get("update_source"))
				ctx.Put("update_type", r.Ctx.Get("update_source")+" (following)")
				for _, selector := range item.Follow {
					url, ok := jsonParsed.Path(selector).Data().(string)
					if ok {
						source.Request("GET", url, nil, ctx, source.Headers)
					}
				}
			})
			itemURL := baseURL
			itemURL.Path = itemURL.Path + item.Path
			ctx := colly.NewContext()
			ctx.Put("update_source", "initial")
			ctx.Put("update_type", "initial")
			go func() {
				err := source.Request("GET", itemURL.String(), nil, ctx, source.Headers)
				if err != nil {
					errs <- errors.Wrap(err, "initial update request failure")
				}
			}()
			// The initial run is on it's way, now we need to set up timers for subsequent full and incremental runs.
			frequency, err := time.ParseDuration(item.Update.FullFrequency)
			if err != nil {
				return errors.Wrap(err, "could not parse full update frequency")
			}
			fullTicker := time.NewTicker(frequency)
			go func() {
				for range fullTicker.C {
					ctx := colly.NewContext()
					ctx.Put("update_source", "full")
					ctx.Put("update_type", "full")
					err := source.Request("GET", itemURL.String(), nil, ctx, source.Headers)
					if err != nil {
						errs <- errors.Wrap(err, "full update request failure")
					}
				}
			}()
			frequency, err = time.ParseDuration(item.Update.IncrementalFrequency)
			if err != nil {
				return errors.Wrap(err, "could not parse incremental update frequency")
			}
			incrementalTicker := time.NewTicker(frequency)
			go func() {
				for range incrementalTicker.C {
					ctx := colly.NewContext()
					ctx.Put("update_source", "incremental")
					ctx.Put("update_type", "incremental")
					// Make a copy to avoid updating itemURL.
					updateURL, _ := url.Parse(itemURL.String())
					q := updateURL.Query()
					item.watermarkLock.RLock()
					q.Set(item.Update.IncrementalKey, fmt.Sprintf(item.Update.IncrementalValue, item.watermark.Format(item.Update.IncrementalFormat)))
					item.watermarkLock.RUnlock()
					updateURL.RawQuery = q.Encode()
					err := source.Request("GET", updateURL.String(), nil, ctx, source.Headers)
					if err != nil {
						errs <- errors.Wrap(err, "incremental update request failure")
					}
				}
			}()
		}
	}
	// Block here until we recieve an error.
	err := <-errs
	return err
}
