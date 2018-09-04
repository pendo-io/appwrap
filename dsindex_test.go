package appwrap

import (
	"fmt"
	"io/ioutil"
	"os"

	. "gopkg.in/check.v1"
)

var _ = fmt.Printf

const testIndex = `
indexes:

- kind: entityKind
  properties:
    - name: fieldB
    - name: fieldA

- kind: entity2
  properties:
    - name: backwards
      direction: desc
    - name: normal

- kind: entityKind
  ancestor: yes
  properties:
    - name: otherField

`

func (dsit *AppengineInterfacesTest) TestLoadIndexYaml(c *C) {
	idx, err := LoadIndex([]byte(testIndex))
	c.Assert(err, IsNil)
	c.Assert(idx, DeepEquals, DatastoreIndex{
		"entityKind": []entityIndex{
			{
				fields: map[string]fieldIndex{
					"fieldA": {index: 1},
					"fieldB": {index: 0},
				},
			},
			{
				ancestor: true,
				fields: map[string]fieldIndex{
					"otherField": {index: 0},
				},
			},
		},
		"entity2": []entityIndex{
			{
				fields: map[string]fieldIndex{
					"backwards": {descending: true},
					"normal":    {index: 1},
				},
			},
		},
	})
}
