// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package requester provides commands to run load tests and display results.
package requester

import (
	"bytes"
	"crypto/tls"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync"
	"time"
	"fmt"
       "strconv"
       "strings"
       "math/rand"

	"golang.org/x/net/http2"
    "encoding/json"
)

// Max size of the buffer of result channel.
const maxResult = 1000000
const maxIdleConn = 500

type result struct {
	err           error
	statusCode    int
	offset        time.Duration
	duration      time.Duration
	connDuration  time.Duration // connection setup(DNS lookup + Dial up) duration
	dnsDuration   time.Duration // dns lookup duration
	reqDuration   time.Duration // request "write" duration
	resDuration   time.Duration // response "read" duration
	delayDuration time.Duration // delay between response and request
	contentLength int64
	id            string
}

type Work struct {
	// Request is the request to be made.
	Request *http.Request

	RequestBody []byte

	ReqBodyLines [] string
    QueryLines [] string

	// RequestFunc is a function to generate requests. If it is nil, then
	// Request and RequestData are cloned for each request.
	RequestFunc func() *http.Request

	// N is the total number of requests to make.
	N int

	// C is the concurrency level, the number of concurrent workers to run.
	C int

	// H2 is an option to make HTTP/2 requests
	H2 bool

	// Timeout in seconds.
	Timeout int

	// Qps is the rate limit in queries per second.
	QPS float64

	// DisableCompression is an option to disable compression in response
	DisableCompression bool

	// DisableKeepAlives is an option to prevents re-use of TCP connections between different HTTP requests
	DisableKeepAlives bool

	// DisableRedirects is an option to prevent the following of HTTP redirects
	DisableRedirects bool

	// Output represents the output type. If "csv" is provided, the
	// output will be dumped as a csv stream.
	Output string

        Input   string
        inputData       []string
	// ProxyAddr is the address of HTTP proxy server in the format on "host:port".
	// Optional.
	ProxyAddr *url.URL

	// Writer is where results will be written. If nil, results are written to stdout.
	Writer io.Writer

	initOnce sync.Once
	results  chan *result
	stopCh   chan struct{}
	start    time.Duration

	report *report
    
    opFile *os.File
    m *sync.Mutex 
}


func (b *Work) writer() io.Writer {
	if b.Writer == nil {
		return os.Stdout
	}
	if len(b.Input) > 0 {
               dat, err := ioutil.ReadFile(b.Input)
               if err != nil {
                       panic(err)
               }
               b.inputData = strings.Split(string(dat),"\n")
       }
	return b.Writer
}

// Init initializes internal data-structures
func (b *Work) Init() {
	b.initOnce.Do(func() {
		b.results = make(chan *result, min(b.C*1000, maxResult))
		b.stopCh = make(chan struct{}, b.C)
	})
}

// Run makes all the requests, prints the summary. It blocks until
// all work is done.
func (b *Work) Run() {
	b.Init()
	b.start = now()
	b.report = newReport(b.writer(), b.results, b.Output, b.N)
	// Run the reporter first, it polls the result channel until it is closed.
	go func() {
		runReporter(b.report)
	}()
	b.runWorkers()
	b.Finish()
}

func (b *Work) Stop() {
	// Send stop signal so that workers can stop gracefully.
	for i := 0; i < b.C; i++ {
		b.stopCh <- struct{}{}
	}
}

func (b *Work) Finish() {
	close(b.results)
	total := now() - b.start
	// Wait until the reporter is done.
	<-b.report.done
	b.report.finalize(total)
}

func (b *Work) makeRequest(c *http.Client,workerId int, n2 int, nPerWorker int) {
	s := now()
	var size int64
	var code int
	var dnsStart, connStart, resStart, reqStart, delayStart time.Duration
	var dnsDuration, connDuration, resDuration, reqDuration, delayDuration time.Duration
	var req *http.Request
	var id string
    var Extra string

       Extra=""
       id=strconv.Itoa(workerId)+","+strconv.Itoa(n2)+","
       var rIndex int
       if len(b.inputData) > 0 {
               rIndex=rand.Intn(len(b.inputData)-1)
               Extra=b.inputData[rIndex]
               id=id+Extra+","
       }
	if b.RequestFunc != nil {
		req = b.RequestFunc()
	} else {

		// if len(b.ReqBodyLines) > 0 {
  //   		//var reqBody []byte
  //   		var reqBodyIdx = workerId * nPerWorker + n2 
  //   		var reqBody = []byte(b.ReqBodyLines[reqBodyIdx])
  //   		req = cloneRequest(b.Request, reqBody,workerId,n2,b.Output,Extra)
		// } else {

  //           req = cloneRequest(b.Request, b.RequestBody,workerId,n2,b.Output,Extra)
  //      	}
        var reqBody []byte = b.RequestBody
        var queryObj map[string]interface{}

        if len(b.ReqBodyLines) > 0 {
            //var reqBody []byte
            var reqBodyIdx = workerId * nPerWorker + n2 
            fmt.Printf("\nreqBodyIdx = %d, wid = %d, nPerWorker = %d, n2 = %d", reqBodyIdx, workerId, nPerWorker, n2);
            reqBody = []byte(b.ReqBodyLines[reqBodyIdx])
        }

        req = cloneRequest(b.Request, reqBody,workerId,n2,b.Output,Extra)
        
        var reqIdx = workerId * nPerWorker + n2 
        if len(b.QueryLines) > reqIdx {
        
            var queryObjStr = []byte(b.QueryLines[reqIdx]);
            err := json.Unmarshal(queryObjStr, &queryObj)
            if err != nil {
                fmt.Println("error in parsing query params list:", err)
            } else {
                var q = url.Values{}
                for key, value := range queryObj {
                    
                    if s, ok := value.(string); ok {
                        // s is string here
                        q.Add(key, s)
                    } else if v, ok := value.(float64); ok {
                        //v is int32 here
                        strV := fmt.Sprintf("%f", v) 
                        q.Add(key, strV)
                    }                     
                    
                    
                }
                req.URL.RawQuery = q.Encode()
            }            

        }




	}
	trace := &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = now()
		},
		DNSDone: func(dnsInfo httptrace.DNSDoneInfo) {
			dnsDuration = now() - dnsStart
		},
		GetConn: func(h string) {
			connStart = now()
		},
		GotConn: func(connInfo httptrace.GotConnInfo) {
			if !connInfo.Reused {
				connDuration = now() - connStart
			}
			reqStart = now()
		},
		WroteRequest: func(w httptrace.WroteRequestInfo) {
			reqDuration = now() - reqStart
			delayStart = now()
			id = id + time.Now().String()
		},
		GotFirstResponseByte: func() {
			delayDuration = now() - delayStart
			resStart = now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := c.Do(req)
	if err == nil {
		size = resp.ContentLength
		code = resp.StatusCode
        
        b.m.Lock() 
            io.Copy(b.opFile, resp.Body)
            b.opFile.WriteString("\n")
        b.m.Unlock()
        
		//io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	t := now()
	resDuration = t - resStart
	finish := t - s
	b.results <- &result{
		offset:        s,
		statusCode:    code,
		duration:      finish,
		err:           err,
		contentLength: size,
		connDuration:  connDuration,
		dnsDuration:   dnsDuration,
		reqDuration:   reqDuration,
		resDuration:   resDuration,
		delayDuration: delayDuration,
		id: id,
	}
}

func (b *Work) runWorker(client *http.Client, n int, id int, nPerWorker int) {
	var throttle <-chan time.Time
	if b.QPS > 0 {
		throttle = time.Tick(time.Duration(1e6/(b.QPS)) * time.Microsecond)
	}

	if b.DisableRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	for i := 0; i < n; i++ {
		// Check if application is stopped. Do not send into a closed channel.
		select {
		case <-b.stopCh:
			return
		default:
			if b.QPS > 0 {
				<-throttle
			}
			b.makeRequest(client,id,i, n)
		}
	}
}

func (b *Work) runWorkers() {
	var wg sync.WaitGroup
	wg.Add(b.C)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         b.Request.Host,
		},
		MaxIdleConnsPerHost: min(b.C, maxIdleConn),
		DisableCompression:  b.DisableCompression,
		DisableKeepAlives:   b.DisableKeepAlives,
		Proxy:               http.ProxyURL(b.ProxyAddr),
	}
	if b.H2 {
		http2.ConfigureTransport(tr)
	} else {
		tr.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	client := &http.Client{Transport: tr, Timeout: time.Duration(b.Timeout) * time.Second}

	// Ignore the case where b.N % b.C != 0.

    //open an output file where we record the response body
    
    opFile, err := os.Create("./op.txt")
    if err != nil {
        panic(err)
    }
    b.opFile = opFile    
    defer b.opFile.Close()

    var mut sync.Mutex
    b.m = &mut;

	for i := 0; i < b.C; i++ {
		go func(n int) {
			b.runWorker(client, b.N/b.C,n, b.C)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request, body []byte, n int,n2 int, Output string,Extra string) *http.Request {
	// shallow copy of the struct
	r2 := new(http.Request)
	*r2 = *r
	// deep copy of the Header
	r2.Header = make(http.Header, len(r.Header))
	for k, s := range r.Header {
		r2.Header[k] = append([]string(nil), s...)
	}
	if len(body) > 0 {
		r2.Body = ioutil.NopCloser(bytes.NewReader(body))
	}

       if Output == "csv" {
               var appender string
               if strings.Contains(r.URL.String(), "?") {
                       appender = "&"
               } else {
                       appender = "?"
               }
               var newURL =r.URL.String()+appender+"conn="+strconv.Itoa(n)+"&cr="+strconv.Itoa(n2)+"&"+Extra
               r2,err := http.NewRequest(r.Method,newURL,nil)
               if err != nil {
                       fmt.Println("error")
               }
               // deep copy of the Header
               r2.Header = make(http.Header, len(r.Header))
               for k, s := range r.Header {
                       r2.Header[k] = append([]string(nil), s...)
               }
               if len(body) > 0 {
                       r2.Body = ioutil.NopCloser(bytes.NewReader(body))
                       r2.ContentLength = int64(len(body))
               }
               return r2
       } else {
               if len(Extra) > 0 {
                       var appender string
                       if strings.Contains(r.URL.String(), "?") {
                               appender = "&"
                       } else {
                               appender = "?"
                       }
                       var newURL =r.URL.String()+appender+Extra
                       r2,err := http.NewRequest(r.Method,newURL,nil)
                       if err != nil {
                               fmt.Println("error")
                       }
                       // deep copy of the Header
                       r2.Header = make(http.Header, len(r.Header))
                       for k, s := range r.Header {
                               r2.Header[k] = append([]string(nil), s...)
                       }
                       if len(body) > 0 {
                               r2.Body = ioutil.NopCloser(bytes.NewReader(body))
                       }
                       return r2
               }
               r2 := new(http.Request)
               *r2 = *r
               // deep copy of the Header
               r2.Header = make(http.Header, len(r.Header))
               for k, s := range r.Header {
                       r2.Header[k] = append([]string(nil), s...)
               }
               if len(body) > 0 {
                       r2.Body = ioutil.NopCloser(bytes.NewReader(body))
               }
	return r2
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
