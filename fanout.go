// Copyright (c) 2020 Doc.ai and/or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fanout

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/debug"
	"github.com/coredns/coredns/plugin/dnstap"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

var log = clog.NewWithPlugin("fanout")

// Fanout represents a plugin instance that can do async requests to list of DNS servers.
type Fanout struct {
	clients        []Client
	tlsConfig      *tls.Config
	excludeDomains Domain
	tlsServerName  string
	timeout        time.Duration
	net            string
	from           string
	attempts       int
	workerCount    int
	tapPlugin      *dnstap.Dnstap
	Next           plugin.Handler
}

// New returns reference to new Fanout plugin instance with default configs.
func New() *Fanout {
	return &Fanout{
		tlsConfig:      new(tls.Config),
		net:            "udp",
		attempts:       3,
		timeout:        defaultTimeout,
		excludeDomains: NewDomain(),
	}
}

func (f *Fanout) addClient(p Client) {
	f.clients = append(f.clients, p)
	f.workerCount++
}

// Name implements plugin.Handler.
func (f *Fanout) Name() string {
	return "fanout"
}

// ServeDNS implements plugin.Handler.
func (f *Fanout) ServeDNS(ctx context.Context, w dns.ResponseWriter, m *dns.Msg) (int, error) {
	req := request.Request{W: w, Req: m}
	if !f.match(&req) {
		return plugin.NextOrFailure(f.Name(), f.Next, ctx, w, m)
	}
	timeoutContext, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	clientCount := len(f.clients)
	workerChannel := make(chan Client, f.workerCount)
	defer close(workerChannel)
	responseCh := make(chan *response, clientCount)
	go func() {
		for i := 0; i < clientCount; i++ {
			client := f.clients[i]
			workerChannel <- client
		}
	}()
	for i := 0; i < f.workerCount; i++ {
		go func() {
			for c := range workerChannel {
				responseCh <- f.processClient(timeoutContext, c, &request.Request{W: w, Req: m})
			}
		}()
	}
	result := f.getFanoutResult(timeoutContext, responseCh)
	if result == nil {
		return dns.RcodeServerFailure, timeoutContext.Err()
	}
	if result.err != nil {
		return dns.RcodeServerFailure, result.err
	}
	// dnsTAP := toDnstap(ctx, result.client.Endpoint(), f.net, &req, result.response, result.start)
	dnsTAP := toDnstap2(ctx, f.tapPlugin, result.client.Endpoint(), f.net, w.RemoteAddr(), &req, result)

	if !req.Match(result.response) {
		debug.Hexdumpf(result.response, "Wrong reply for id: %d, %s %d", result.response.Id, req.QName(), req.QType())
		formerr := new(dns.Msg)
		formerr.SetRcode(req.Req, dns.RcodeFormatError)
		logErrIfNotNil(w.WriteMsg(formerr))
		return 0, dnsTAP
	}
	logErrIfNotNil(w.WriteMsg(result.response))
	return 0, dnsTAP
}

func (f *Fanout) getFanoutResult(ctx context.Context, responseCh <-chan *response) *response {
	count := len(f.clients)
	var result *response
	for {
		select {
		case <-ctx.Done():
			return result
		case r := <-responseCh:
			count--
			if isBetter(result, r) {
				result = r
			}
			if count == 0 {
				return result
			}
			if r.err != nil {
				break
			}
			if r.response.Rcode != dns.RcodeSuccess {
				break
			}
			return r
		}
	}
}

func (f *Fanout) match(state *request.Request) bool {
	if !plugin.Name(f.from).Matches(state.Name()) || f.excludeDomains.Contains(state.Name()) {
		return false
	}
	return true
}

func (f *Fanout) processClient(ctx context.Context, c Client, r *request.Request) *response {
	start := time.Now()
	for j := 0; j < f.attempts || f.attempts == 0; <-time.After(attemptDelay) {
		if ctx.Err() != nil {
			return &response{client: c, response: nil, start: start, err: ctx.Err()}
		}
		msg, err := c.Request(ctx, r)
		if err == nil {
			return &response{client: c, response: msg, start: start, err: err}
		}
		if f.attempts != 0 {
			j++
		}
	}
	return &response{client: c, response: nil, start: start, err: errors.New("attempt limit has been reached")}
}
