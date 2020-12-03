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
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/coredns/coredns/plugin/dnstap"
	"github.com/coredns/coredns/plugin/dnstap/msg"
	"github.com/coredns/coredns/request"

	tap "github.com/dnstap/golang-dnstap"
)

var (
	protoUDP    = tap.SocketProtocol_UDP
	protoTCP    = tap.SocketProtocol_TCP
	familyINET  = tap.SocketFamily_INET
	familyINET6 = tap.SocketFamily_INET6
)

func logErrIfNotNil(err error) {
	if err == nil {
		return
	}
	log.Error(err)
}

func toDnstap2(ctx context.Context, tapPlugin *dnstap.Dnstap, host, protocol string, remote net.Addr, state *request.Request, resp *response) error {
	if tapPlugin == nil {
		return nil
	}

	q := new(tap.Message)
	msg.SetQueryTime(q, resp.start)
	err := setQueryHostPort(q, host, protocol)
	if err != nil {
		return err
	}

	if tapPlugin.IncludeRawMessage {
		buf, _ := state.Req.Pack()
		q.QueryMessage = buf
	}
	msg.SetType(q, tap.Message_FORWARDER_QUERY)
	tapPlugin.TapMessage(q)

	if resp.response != nil {
		r := new(tap.Message)
		msg.SetResponseTime(r, time.Now())
		err = msg.SetResponseAddress(r, remote)
		if err != nil {
			return err
		}
		msg.SetType(r, tap.Message_FORWARDER_RESPONSE)
		tapPlugin.TapMessage(q)
	}

	return nil
}

func setQueryHostPort(t *tap.Message, addr string, protocol string) error {
	ip, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	p, err := strconv.ParseUint(port, 10, 32)
	if err != nil {
		return err
	}

	v := uint32(p)
	t.QueryPort = &v

	if protocol == "tcp" {
		t.SocketProtocol = &protoTCP
	} else {
		t.SocketProtocol = &protoUDP
	}

	if ip := net.ParseIP(ip); ip != nil {
		t.QueryAddress = []byte(ip)
		if ip := ip.To4(); ip != nil {
			t.SocketFamily = &familyINET
		} else {
			t.SocketFamily = &familyINET6
		}
		return nil
	}

	return errors.New("not an ip address")
}
