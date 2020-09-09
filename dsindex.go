package appwrap

import (
	"context"
	"fmt"
	"reflect"

	admin "cloud.google.com/go/datastore/admin/apiv1"
	"google.golang.org/api/iterator"
	dsadmin "google.golang.org/genproto/googleapis/datastore/admin/v1"
	"gopkg.in/yaml.v2"
)

type Properties struct {
	Name      string
	Direction string
}

type indexYaml struct {
	Indexes []struct {
		Kind       string
		Ancestor   bool
		Properties []Properties
	}
}

type fieldIndex struct {
	descending bool
	index      int
}

func (fi fieldIndex) String() string {
	prefix := ""
	if fi.descending {
		prefix = "-"
	}

	return fmt.Sprintf("%s[%d]", prefix, fi.index)
}

type entityIndex struct {
	ancestor bool
	fields   map[string]fieldIndex
}

func (ei entityIndex) String() string {
	s := ""
	for fieldName, field := range ei.fields {
		if len(s) > 0 {
			s += " "
		}
		s += field.String() + fieldName
	}
	return s
}

type DatastoreIndex map[string][]entityIndex

type datastoreAdminClient struct {
	client  *admin.DatastoreAdminClient
	context context.Context
}

func NewDatastoreAdminClient(ctx context.Context) datastoreAdminClient {
	c, err := admin.NewDatastoreAdminClient(ctx)
	if err != nil {
		panic(fmt.Sprintf("Error creating DatastoreAdminClient: %s", err))
	}

	dac := datastoreAdminClient{
		client:  c,
		context: ctx,
	}
	return dac
}

func (c datastoreAdminClient) GetDatastoreIndex(project string, readyOnly bool) (DatastoreIndex, error) {
	req := &dsadmin.ListIndexesRequest{
		ProjectId: project,
	}

	it := c.client.ListIndexes(c.context, req)
	for {
		_, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return DatastoreIndex{}, fmt.Errorf("Error listing indexes: %s", err)
		}
	}

	indexes := it.Response.(*dsadmin.ListIndexesResponse)

	return LoadIndexDatastore(*indexes, readyOnly)
}

func LoadIndexYaml(data []byte) (DatastoreIndex, error) {
	var indexConfig indexYaml
	if err := yaml.Unmarshal(data, &indexConfig); err != nil {
		return nil, err
	}

	index := make(DatastoreIndex, len(indexConfig.Indexes))
	for _, spec := range indexConfig.Indexes {
		if spec.Kind == "" {
			return nil, fmt.Errorf("missing entity kind")
		} else if len(spec.Properties) < 2 && !spec.Ancestor {
			if len(spec.Properties) == 1 && spec.Properties[0].Name == "__key__" && spec.Properties[0].Direction == "desc" {
				// this is okay
			} else {
				return nil, fmt.Errorf("< 2 properties for index for kind " + spec.Kind)
			}
		}

		entIndex := entityIndex{ancestor: spec.Ancestor, fields: make(map[string]fieldIndex, len(spec.Properties))}
		for i, prop := range spec.Properties {
			if prop.Name == "" {
				return nil, fmt.Errorf("missing field name for kind " + spec.Kind)
			} else if prop.Direction != "" && prop.Direction != "desc" && prop.Direction != "asc" {
				return nil, fmt.Errorf(`unknown direction "%s" for kind %s`, prop.Direction, spec.Kind)
			} else {
				entIndex.fields[prop.Name] = fieldIndex{
					descending: prop.Direction == "desc",
					index:      i,
				}
			}
		}

		index[spec.Kind] = append(index[spec.Kind], entIndex)
	}

	return index, nil
}

func LoadIndexDatastore(resp dsadmin.ListIndexesResponse, readyOnly bool) (DatastoreIndex, error) {
	index := make(DatastoreIndex, len(resp.Indexes))
	for _, spec := range resp.Indexes {
		if !readyOnly || spec.State == dsadmin.Index_READY {
			entIndex := entityIndex{ancestor: spec.Ancestor == dsadmin.Index_ALL_ANCESTORS, fields: make(map[string]fieldIndex, len(spec.Properties))}
			for i, prop := range spec.Properties {
				entIndex.fields[prop.Name] = fieldIndex{
					descending: prop.Direction == dsadmin.Index_DESCENDING,
					index:      i,
				}
			}
			index[spec.Kind] = append(index[spec.Kind], entIndex)
		}
	}
	return index, nil
}

func IndexIntersection(d1 DatastoreIndex, d2 DatastoreIndex) DatastoreIndex {
	intersection := make(DatastoreIndex, len(d1))
	for d1Entity, d1Indexes := range d1 {
		if d2Indexes, ok := d2[d1Entity]; ok {
			for _, d1Index := range d1Indexes {
				for _, d2Index := range d2Indexes {
					if reflect.DeepEqual(d2Index, d1Index) {
						intersection[d1Entity] = append(intersection[d1Entity], d1Index)
					}
				}
			}
		}

	}

	return intersection
}
