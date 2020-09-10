package appwrap

import (
	"fmt"

	dsadmin "google.golang.org/genproto/googleapis/datastore/admin/v1"
	. "gopkg.in/check.v1"
	"github.com/stretchr/testify/mock"
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

type datastoreAdminAdapterMock struct {
	mock.Mock
}

func (d datastoreAdminAdapterMock) withEachIndexFrom(request *dsadmin.ListIndexesRequest, f func(index *dsadmin.Index)) error {
	panic("implement me")
}

func (dsit *AppengineInterfacesTest) TestLoadIndexYaml(c *C) {
	idx, err := LoadIndexYaml([]byte(testIndex))
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

func (dsit *AppengineInterfacesTest) TestGetDatastoreIndex(c *C) {
	d := dsadmin.ListIndexesResponse{
		Indexes: []*dsadmin.Index{
			{
				Kind: "entityKind",
				Properties: []*dsadmin.Index_IndexedProperty{
					{
						Name: "fieldB",
					},
					{
						Name: "fieldA",
					},
				},
				State: dsadmin.Index_READY,
			},
			{
				Kind: "entity2",
				Properties: []*dsadmin.Index_IndexedProperty{
					{
						Name:      "backwards",
						Direction: dsadmin.Index_DESCENDING,
					},
					{
						Name: "normal",
					},
				},
				State: dsadmin.Index_READY,
			},
			{
				Kind:     "entityKind",
				Ancestor: dsadmin.Index_ALL_ANCESTORS,
				Properties: []*dsadmin.Index_IndexedProperty{
					{
						Name: "otherField",
					},
				},
				State: dsadmin.Index_READY,
			},
			{
				Kind:     "entityKind",
				Ancestor: dsadmin.Index_ALL_ANCESTORS,
				Properties: []*dsadmin.Index_IndexedProperty{
					{
						Name: "notReady",
					},
				},
				State: dsadmin.Index_CREATING,
			},
		},
	}

	adapterMock := &datastoreAdminAdapterMock{
		mock.Mock{},
	}
	client := datastoreAdminClient{
		adapter: adapterMock,
	}
	idx, err := client.GetDatastoreIndex("project", true)
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

func (dsit *AppengineInterfacesTest) TestIndexIntersection(c *C) {
	d1 := DatastoreIndex{
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
	}

	d2 := DatastoreIndex{
		"entityKind": []entityIndex{
			{
				fields: map[string]fieldIndex{
					"fieldB": {index: 0},
					"fieldA": {index: 1},
				},
			},
			{
				ancestor: false,
				fields: map[string]fieldIndex{
					"otherField": {index: 0},
				},
			},
		},
		"entity3": []entityIndex{
			{
				fields: map[string]fieldIndex{
					"backwards": {descending: true},
					"normal":    {index: 1},
				},
			},
		},
	}

	idx := IndexIntersection(d1, d2)
	c.Assert(idx, DeepEquals, DatastoreIndex{
		"entityKind": []entityIndex{
			{
				fields: map[string]fieldIndex{
					"fieldA": {index: 1},
					"fieldB": {index: 0},
				},
			},
		},
	})
}
