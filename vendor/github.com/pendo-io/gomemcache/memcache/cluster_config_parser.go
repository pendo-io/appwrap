/*
Copyright 2020 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memcache

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

var (
	configKeyword = "CONFIG"
)

// ClusterNode represents address of a memcached node.
type ClusterNode struct {
	Host string
	Port int64
}

// ClusterConfig represents cluster configuration which contains nodes and version.
type ClusterConfig struct {
	// ConfigId is the monotonically increasing identifier for the config information
	ConfigID int64

	// NodeAddresses are array of ClusterNode which contain address of a memcache node.
	NodeAddresses []ClusterNode
}

// parseConfigGetResponse reads a CONFIG GET response from r and calls cb for each
// read and allocates ClusterConfig
func parseConfigGetResponse(r *bufio.Reader, cb func(*ClusterConfig)) error {
	scanner := bufio.NewScanner(r)
	clusterConfig := new(ClusterConfig)
	// TODO-GO: Replace below document with Feature on github describing the change.
	// Response from config service is here:
	// https://docs.google.com/document/d/15V9tKuffWrcCVwDZRmRBOV1SDcuo6P8u05dddwZYxCo/edit
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		// Skip empty line
		if line == "" {
			continue
		}
		// CONFIG keyword line is expected as follows:
		// CONFIG cluster 0 <count-of-bytes-in-next-two-lines>\r\n
		if strings.Contains(line, configKeyword) {
			// After config keyword we expect next line to contain config id in the form
			// <config-id>\n
			scanner.Scan()
			configIDLine := strings.TrimSpace(scanner.Text())
			configID, parseError := strconv.ParseInt(configIDLine, 10, 64)
			if parseError != nil {
				return parseError
			}
			clusterConfig.ConfigID = configID

			// Read the third line of the response which contains host
			// hostname1|ip-address1|port1<space>hostname2|ip-address2|port2<space>\n\r\n
			scanner.Scan()
			nodeHostPortAdds := strings.TrimSpace(scanner.Text())
			// tokenize on space and then pipe
			nodes := strings.Split(nodeHostPortAdds, " ")
			for _, node := range nodes {
				nodeHostPort := strings.Split(node, "|")
				if len(nodeHostPort) < 3 {
					return fmt.Errorf("host address (%s) not in expected format", nodeHostPort)
				}
				nodePort, intParseError := strconv.ParseInt(nodeHostPort[2], 10, 64)
				if intParseError != nil {
					return intParseError
				}
				clusterConfig.NodeAddresses = append(clusterConfig.NodeAddresses, ClusterNode{Host: nodeHostPort[1], Port: nodePort})
			}
		}

	}
	cb(clusterConfig)
	return nil
}
