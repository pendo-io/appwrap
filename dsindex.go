package appwrap

import (
	"fmt"

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

func LoadIndex(data []byte) (DatastoreIndex, error) {
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
