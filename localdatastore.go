package appwrap

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
)

type dsItem struct {
	props []AppwrapProperty
	key   *DatastoreKey
}

type dsItemListSorter struct {
	l      []*dsItem
	fields []string
}

func cmp(aI, bI interface{}) int {
	r := _cmp(aI, bI)
	//fmt.Printf("%s cmp %s = %d\n", aI, bI, r)
	return r
}

func typeConvert(a interface{}) interface{} {
	switch a.(type) {
	case int:
		return int64(a.(int))
	case int32:
		return int64(a.(int32))
	case int64:
		return a
	case string:
		return a
	case *DatastoreKey:
		return a
	case time.Time:
		return a
	}

	strType := reflect.TypeOf("")
	val := reflect.ValueOf(a)
	if val.Type().ConvertibleTo(strType) {
		a = val.Convert(strType).Interface().(string)
	}

	return a
}

func _cmp(aI, bI interface{}) int {
	aI, bI = typeConvert(aI), typeConvert(bI)

	switch aI.(type) {
	case bool:
		if aI.(bool) == bI.(bool) {
			return 0
		} else if aI.(bool) == false {
			return -1
		} else {
			return 1
		}
	case int:
		if aI.(int) < bI.(int) {
			return -1
		} else if aI.(int) == bI.(int) {
			return 0
		} else {
			return 1
		}
	case int64:
		var b int64
		switch bI.(type) {
		case int64:
			b = bI.(int64)
		case int32:
			b = int64(bI.(int32))
		case int:
			b = int64(bI.(int))
		default:
			panic(fmt.Sprintf("bad type %T for int64 conversion", bI))
		}

		if aI.(int64) < b {
			return -1
		} else if aI.(int64) == b {
			return 0
		} else {
			return 1
		}
	case *DatastoreKey:
		a, b := aI.(*DatastoreKey), bI.(*DatastoreKey)
		if a == b {
			return 0
		} else if a == (*DatastoreKey)(nil) {
			return -1
		} else if b == (*DatastoreKey)(nil) {
			return 1
		} else if KeyKind(a) < KeyKind(b) {
			return -1
		} else if KeyKind(a) > KeyKind(b) {
			return 1
		} else if KeyIntID(a) < KeyIntID(b) || KeyStringID(a) < KeyStringID(b) {
			return -1
		} else if KeyIntID(a) > KeyIntID(b) || KeyStringID(a) > KeyStringID(b) {
			return 1
		} else {
			return _cmp(KeyParent(a), KeyParent(b))
		}
	case time.Time:
		if aI.(time.Time).Before(bI.(time.Time)) {
			return -1
		} else if aI.(time.Time).Equal(bI.(time.Time)) {
			return 0
		} else {
			return 1
		}
	case string:
		if aI.(string) < bI.(string) {
			return -1
		} else if aI.(string) == bI.(string) {
			return 0
		} else {
			return 1
		}
	}

	panic(fmt.Sprintf("unsupported type %T for ordering", aI))
}

func (a dsItemListSorter) Len() int      { return len(a.l) }
func (a dsItemListSorter) Swap(i, j int) { a.l[i], a.l[j] = a.l[j], a.l[i] }
func (a dsItemListSorter) Less(i, j int) bool {
	t := a.less(i, j)
	//fmt.Printf("%s < %s = %t (%s)\n", a.l[i], a.l[j], t, a.fields)
	return t
}
func (a dsItemListSorter) less(i, j int) bool {
	for _, field := range a.fields {
		reverseSense := false
		if field[0] == '-' {
			reverseSense = true
			field = field[1:]
		}

		var aVal interface{}
		var bVal interface{}
		if field == "__key__" {
			aVal = a.l[i].key
			bVal = a.l[j].key
		} else {
			for x := range a.l[i].props {
				if a.l[i].props[x].Name == field {
					aVal = a.l[i].props[x].Value
				}
			}

			for x := range a.l[j].props {
				if a.l[j].props[x].Name == field {
					bVal = a.l[j].props[x].Value
				}
			}
		}

		result := 0
		if aVal == nil && bVal == nil {
			result = 0
		} else if aVal == nil {
			result = -1
		} else if bVal == nil {
			result = 1
		} else {
			//fmt.Printf("COMPARING %s to %s", aVal, bVal)
			result = cmp(aVal, bVal)
		}

		if result == -1 && reverseSense {
			return false
		} else if result == -1 {
			return true
		} else if result == 1 && reverseSense {
			return true
		} else if result == 1 {
			return false
		}
	}

	return false
}

func (item *dsItem) cp(dst interface{}, fields map[string]bool, addField bool) error {
	props := item.props
	if fields != nil {
		props = make([]AppwrapProperty, 0, len(fields))
		for _, prop := range item.props {
			if fields[prop.Name] {
				props = append(props, prop)
			}
		}
	}

	if addField {
		// if you hit an error on this field not being defined you probably want to add a customer Load/Saver
		// that ignores unknown fields
		props = append(props, AppwrapProperty{Name: "_debug_added_field", Value: true})
	}

	//fmt.Printf("%T <- %+v (%d)\n", dst, props, len(item.props))
	if loadSaver, okay := dst.(DatastorePropertyLoadSaver); okay {
		//fmt.Printf("\tload saver\n")
		//fmt.Printf("FILLED %+v\n", dst)
		// Load() may mess with the array; don't let it break our stored data
		propsCopy := make([]AppwrapProperty, len(props))
		copy(propsCopy, props)
		return loadSaver.Load(ToDatastorePropertyList(propsCopy))
	} else {
		return LoadStruct(dst, ToDatastorePropertyList(props))
	}
}

type LocalDatastore struct {
	lastId          int64
	entities        map[string]*dsItem
	emptyContext    context.Context
	mtx             *sync.Mutex
	namespace       string
	namespaces      map[string]*LocalDatastore
	parent          *LocalDatastore
	addEntityFields bool
	index           DatastoreIndex
}

// stubContext is a ridicule-worthy hack that returns a string "s~memds" for ANY
// call to context.Context.Value(). This is just enough to statisfy the
// appengine.Datastore.NewKey() mechanism. We had to do this to deal with Go 1.6,
// because "internal" packages' visibility is now enforced.
type stubCtx int

func (s *stubCtx) Deadline() (deadline time.Time, ok bool) { return }
func (s *stubCtx) Done() <-chan struct{}                   { return nil }
func (s *stubCtx) Err() error                              { return nil }
func (s *stubCtx) Value(key interface{}) interface{}       { return "s~memds" }
func (s *stubCtx) String() string                          { return "stubcontext" }

var stubContext = new(stubCtx)

func StubContext() context.Context {
	return stubContext
}

func NewLocalDatastore(addField bool, index DatastoreIndex) Datastore {
	return &LocalDatastore{
		lastId:          1 << 30,
		entities:        make(map[string]*dsItem),
		emptyContext:    StubContext(),
		mtx:             &sync.Mutex{},
		namespaces:      make(map[string]*LocalDatastore),
		addEntityFields: addField,
		index:           index,
	}
}

func (ds *LocalDatastore) Deadline(t time.Time) Datastore {
	return ds
}

func (ds *LocalDatastore) Namespace(ns string) Datastore {
	if ds.parent != nil {
		ds = ds.parent
	}

	if _, exists := ds.namespaces[ns]; !exists {
		ds.namespaces[ns] = NewLocalDatastore(ds.addEntityFields, nil).(*LocalDatastore)
		ds.namespaces[ns].namespace = ns
		ds.namespaces[ns].parent = ds
	}

	return ds.namespaces[ns]
}

func (ds *LocalDatastore) AllocateIDSet(incompleteKeys []*DatastoreKey) ([]*DatastoreKey, error) {
	return emulateAllocateIDSet(ds, incompleteKeys)
}

func emulateAllocateIDSet(d LegacyDatastore, incompleteKeys []*DatastoreKey) ([]*DatastoreKey, error) {
	kind := ""
	var parent *DatastoreKey

	for _, k := range incompleteKeys {
		if kind == "" {
			kind = KeyKind(k)
			parent = KeyParent(k)
		} else if kind != KeyKind(k) {
			return nil, errors.New("kind mismatch in legacy AllocateIDSet")
		}

		newParent := KeyParent(k)
		if parent == nil && newParent == nil {
			continue
		} else if parent == nil {
			return nil, errors.New("parent mismatch in legacy AllocateIDSet")
		} else if !parent.Equal(newParent) {
			return nil, errors.New("parent mismatch in legacy AllocateIDSet")
		}
	}

	if low, high, err := d.AllocateIDs(kind, parent, len(incompleteKeys)); err != nil {
		return nil, nil
	} else if int(high-low) != len(incompleteKeys) {
		return nil, errors.New("high/low mismatch in emulateAllocateIDSet()")
	} else {
		newKeys := make([]*DatastoreKey, len(incompleteKeys))
		for i := range incompleteKeys {
			newKeys[i] = d.NewKey(kind, "", low+int64(i), parent)
		}

		return newKeys, nil
	}
}

func (ds *LocalDatastore) AllocateIDs(kind string, parent *DatastoreKey, n int) (int64, int64, error) {
	first := ds.lastId
	ds.lastId += int64(n)
	return first, first + int64(n), nil
}

func (ds *LocalDatastore) delete(keyStr string) {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()
	delete(ds.entities, keyStr)
}

func (ds *LocalDatastore) DeleteMulti(keys []*DatastoreKey) (err error) {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()

	for _, k := range keys {
		delete(ds.entities, k.String())
	}

	return
}

func (ds *LocalDatastore) get(keyStr string) (item *dsItem, found bool) {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()
	item, found = ds.entities[keyStr]
	if found {
		for i, prop := range item.props {
			if timeVal, ok := prop.Value.(time.Time); ok {
				item.props[i].Value = timeVal.UTC()
			}
		}
	}
	return
}

func (ds *LocalDatastore) Get(k *DatastoreKey, dst interface{}) error {
	if k.Incomplete() {
		return ErrInvalidKey
	}
	if item, exists := ds.get(k.String()); !exists {
		return ErrNoSuchEntity
	} else {
		return item.cp(dst, nil, ds.addEntityFields)
	}
}

func (ds *LocalDatastore) GetMulti(keys []*DatastoreKey, dstIntf interface{}) error {
	if len(keys) != reflect.ValueOf(dstIntf).Len() {
		return errors.New("keys and dest have different lengths")
	}

	dstValue := reflect.ValueOf(dstIntf)
	errors := false
	multiError := make(MultiError, len(keys))
	for i, k := range keys {
		if err := ds.Get(k, dstValue.Index(i).Addr().Interface()); err != nil {
			multiError[i] = err
			errors = true
		}
	}

	if errors {
		return multiError
	}

	return nil
}

func (ds *LocalDatastore) Kinds() (kinds []string, err error) {
	m := make(map[string]bool)

	for _, item := range ds.entities {
		if !m[KeyKind(item.key)] {
			m[KeyKind(item.key)] = true
			kinds = append(kinds, KeyKind(item.key))
		}
	}

	return
}

func (ds *LocalDatastore) put(keyStr string, item *dsItem) {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()
	for i, prop := range item.props {
		switch prop.Value.(type) {
		case time.Time:
			item.props[i].Value = prop.Value.(time.Time).UTC()
		case *DatastoreEntity:
			// All structs must be flattened before saving to datastore.  This can be achieved by adding
			// `datastore:",flatten"` to struct fields that represent a nested struct.
			panic("cannot save non-flattened structs.  See https://godoc.org/cloud.google.com/go/datastore#hdr-Properties")
		}

		if timeVal, ok := prop.Value.(time.Time); ok {
			item.props[i].Value = timeVal.UTC()
		}
	}
	ds.entities[keyStr] = item
}

func (ds *LocalDatastore) Put(key *DatastoreKey, src interface{}) (*DatastoreKey, error) {
	finalKeyCopy := *key
	finalKey := &finalKeyCopy

	if KeyStringID(finalKey) == "" && KeyIntID(finalKey) == 0 {
		finalKey = ds.NewKey(KeyKind(finalKey), "", ds.lastId, KeyParent(finalKey))
		ds.lastId++
	}

	if saver, okay := src.(DatastorePropertyLoadSaver); okay {
		if item, err := saver.Save(); err != nil {
			return nil, err
		} else {
			k := *finalKey
			ds.put(finalKey.String(), &dsItem{props: ToAppwrapPropertyList(item), key: &k})
		}
	} else if item, err := SaveStruct(src); err != nil {
		return nil, err
	} else {
		k := *finalKey
		ds.put(finalKey.String(), &dsItem{props: ToAppwrapPropertyList(item), key: &k})
	}

	return finalKey, nil
}

func (ds *LocalDatastore) PutMulti(keys []*DatastoreKey, src interface{}) ([]*DatastoreKey, error) {
	srcValue := reflect.ValueOf(src)
	finalKeys := make([]*DatastoreKey, len(keys))
	for i, k := range keys {
		val := srcValue.Index(i)
		if val.Kind() == reflect.Struct {
			val = val.Addr()
		}

		if finalK, err := ds.Put(k, val.Interface()); err != nil {
			return nil, err
		} else {
			finalKeys[i] = finalK
		}
	}

	return finalKeys, nil
}

type mappedCommit struct {
	keyMap map[*PendingKey]*DatastoreKey
}

func (commit mappedCommit) Key(pending *PendingKey) *DatastoreKey {
	return commit.keyMap[pending]
}

type localDatastoreTransaction struct {
	Datastore
	keyMap map[*PendingKey]*DatastoreKey
}

func (lds localDatastoreTransaction) NewQuery(kind string) DatastoreQuery {
	query := lds.Datastore.NewQuery(kind).(*memoryQuery)
	query.inTransaction = true
	return query
}

func (lds localDatastoreTransaction) Put(key *DatastoreKey, src interface{}) (*PendingKey, error) {
	resultKey, err := lds.Datastore.Put(key, src)
	pk := &PendingKey{}
	lds.keyMap[pk] = resultKey
	return pk, err
}

func (lds localDatastoreTransaction) PutMulti(keys []*DatastoreKey, src interface{}) ([]*PendingKey, error) {
	resultKeys, err := lds.Datastore.PutMulti(keys, src)
	if err != nil {
		return nil, err
	}

	pendingKeys := make([]*PendingKey, len(resultKeys))
	for i := range resultKeys {
		pendingKeys[i] = &PendingKey{}
		lds.keyMap[pendingKeys[i]] = resultKeys[i]

	}

	return pendingKeys, err
}

func (ds *LocalDatastore) RunInTransaction(f func(coreds DatastoreTransaction) error, opts ...DatastoreTransactionOption) (Commit, error) {
	// The datastore must be locked while running a transaction, since the transaction will need to
	// put the new entities in place on commit (as well as the new lastId), or just go back to the original
	// datastore on state.
	ds.mtx.Lock()
	defer ds.mtx.Unlock()

	// Create a new datastore; it's mutex is unlocked, since the "original" datastore is locked.
	// We will copy the entities and lastId from the current datastore before running the transaction func().
	dsCopy := &LocalDatastore{
		lastId:       ds.lastId,
		entities:     make(map[string]*dsItem),
		namespace:    ds.namespace,
		emptyContext: StubContext(),
		mtx:          &sync.Mutex{},
	}
	for k, v := range ds.entities {
		dsCopy.entities[k] = v
	}

	// If the transaction fails, just return the error (and unlock the datastore's mutex) with
	// no updates.
	transaction := &localDatastoreTransaction{Datastore: dsCopy, keyMap: make(map[*PendingKey]*DatastoreKey)}
	if err := f(transaction); err != nil {
		return nil, err
	}

	// Put the new entities and lastId in place on "commit".
	ds.entities = dsCopy.entities
	ds.lastId = dsCopy.lastId
	return mappedCommit{keyMap: transaction.keyMap}, nil
}

func (ds *LocalDatastore) NewQuery(kind string) DatastoreQuery {
	return &memoryQuery{localDs: ds, kind: kind, addEntityFields: ds.addEntityFields}
}

type filter struct {
	eqs  []eqValueFilter
	ineq ineqValueFilter
}

func (f filter) cmpSingle(field string, item *dsItem, vf valueFilter) bool {
	if field == "__key__" {
		return vf.cmpValue(item.key)
	}

	for _, prop := range item.props {
		if prop.Name == field {
			if vf.cmpValue(prop.Value) {
				return true
			}
		}
	}

	return false
}

func (f filter) cmp(field string, item *dsItem) bool {
	if !f.cmpSingle(field, item, f.ineq) {
		return false
	}
	for _, eq := range f.eqs {
		if !f.cmpSingle(field, item, eq) {
			return false
		}
	}
	return true
}

func (f *filter) add(op string, val interface{}) {
	if op == "=" {
		f.eqs = append(f.eqs, eqValueFilter{val: val})
	} else {
		f.ineq.ops = append(f.ineq.ops, op)
		f.ineq.threshs = append(f.ineq.threshs, val)
	}
}

func (f filter) clone() filter {
	return filter{
		eqs:  append([]eqValueFilter(nil), f.eqs...),
		ineq: f.ineq.clone(),
	}
}

type valueFilter interface {
	cmpValue(v interface{}) bool
}

type eqValueFilter struct{ val interface{} }

func (f eqValueFilter) cmpValue(v interface{}) bool { return cmp(v, f.val) == 0 }

type ineqValueFilter struct {
	ops     []string
	threshs []interface{}
}

func (f ineqValueFilter) cmpValue(v interface{}) bool {
	matches := true
	for i, thresh := range f.threshs {
		c := cmp(v, thresh)
		switch op := f.ops[i]; op {
		case "<":
			matches = matches && (c == -1)
		case ">":
			matches = matches && (c == 1)
		case "<=":
			matches = matches && (c != 1)
		case ">=":
			matches = matches && (c != -1)
		default:
			panic(fmt.Sprintf("bad operator %s for filter", op))
		}
	}
	return matches
}

func (f ineqValueFilter) clone() ineqValueFilter {
	return ineqValueFilter{
		ops:     append([]string(nil), f.ops...),
		threshs: append([]interface{}(nil), f.threshs...),
	}
}

type memoryCursor *DatastoreKey

var firstItemCursor memoryCursor = &DatastoreKey{}

type distinctKey struct {
	field string
	value interface{}
}

type distinctKeys []distinctKey

func (dk distinctKeys) Len() int           { return len(dk) }
func (dk distinctKeys) Swap(i, j int)      { dk[i], dk[j] = dk[j], dk[i] }
func (dk distinctKeys) Less(i, j int) bool { return dk[i].field < dk[j].field }

func (dk distinctKeys) Hash() string {
	sort.Sort(dk)
	h := sha1.New()
	for i := range dk {
		h.Write([]byte(fmt.Sprintf("%s", dk[i].value)))
	}
	return base64.URLEncoding.EncodeToString(h.Sum(nil))[0:27]
}

type memoryQuery struct {
	localDs         *LocalDatastore
	filters         map[string]filter
	kind            string
	ancestor        *DatastoreKey
	keysOnly        bool
	limit           int
	offset          int
	start           memoryCursor
	order           []string
	project         map[string]bool
	distinct        bool
	addEntityFields bool
	inTransaction   bool
}

type orderField struct {
	field        string
	isDescending bool
}

func (mq memoryQuery) orderFields() []orderField {
	equalityFields := mq.equalityFields()
	orderFields := []orderField{}
	for _, f := range mq.order {
		var of orderField
		if f[0] == '-' {
			of.field = f[1:]
			of.isDescending = true
		} else {
			of.field = f
			of.isDescending = false
		}
		isAlsoEquality := false
		for _, fieldName := range equalityFields {
			if fieldName == of.field {
				isAlsoEquality = true
			}
		}
		if !isAlsoEquality {
			orderFields = append(orderFields, of)
		}
	}
	return orderFields
}

func (mq memoryQuery) equalityFields() []string {
	equalityFields := []string{}
	for field, filter := range mq.filters {
		if len(filter.ineq.ops) == 0 {
			equalityFields = append(equalityFields, field)
		}
	}
	return equalityFields
}

func (mq memoryQuery) inequalityField() (string, error) {
	inequalityField := ""
	for field, filter := range mq.filters {
		if len(filter.ineq.ops) > 0 {
			if inequalityField == "" {
				inequalityField = field
			} else {
				return "", errors.New("multiple inequalities specified")
			}
		}
	}
	return inequalityField, nil
}

func (mq memoryQuery) numFilters() int {
	num := len(mq.filters)
	if mq.ancestor != nil {
		num++
	}
	return num
}

func (mq memoryQuery) needsIndex(debug func(string, ...interface{})) (bool, error) {
	// A simple sort order (only one one field that's not [key descending]
	// is treated differently.
	complicatedOrder := false
	if len(mq.order) > 1 {
		complicatedOrder = true
	} else if len(mq.order) == 1 && mq.order[0] == "-__key__" {
		complicatedOrder = true
	}

	projecting := len(mq.project) > 0
	isOrdered := len(mq.order) > 0
	hasAncestor := mq.ancestor != nil

	orderFields := mq.orderFields()
	equalityFields := mq.equalityFields()
	inequalityField, err := mq.inequalityField()
	if err != nil {
		return false, err
	}
	hasInequality := inequalityField != ""
	hasEquality := len(equalityFields) > 0

	if mq.localDs.index == nil {
		debug("    no index defined")
		return false, nil
	} else if mq.numFilters() == 0 && !complicatedOrder && !projecting {
		debug("    no index needed for no filters")
		return false, nil
	} else if mq.numFilters() == 1 && !isOrdered && !hasAncestor && !projecting {
		debug("    no index needed on single field")
		return false, nil
	} else if mq.numFilters() == 1 && len(orderFields) == 1 {
		field := ""
		for f := range mq.filters {
			field = f
		}
		if orderFields[0].field == field {
			debug("    single field with order that matches field name")
			return false, nil
		}
	}

	if !hasInequality && !isOrdered && !projecting {
		debug("    only using equality and ancestor filters")
		return false, nil
	} else if hasInequality && !hasEquality && !isOrdered && !hasAncestor && !projecting {
		debug("    only using inequality filter")
		return false, nil
	} else if !isOrdered && inequalityField == "__key__" {
		debug("    only using ancestor filters, equality filters on properties, and inequality filters on keys")
		return false, nil
	}

	if mq.numFilters() == 0 && len(mq.project) == 1 && len(orderFields) < 2 {
		if len(orderFields) == 0 {
			debug("    only projecting on single field")
			return false, nil
		}
		for field := range mq.project {
			if field == orderFields[0].field {
				debug("    single field only projecting and ordering")
				return false, nil
			}
		}
	}

	debug("    needs index")
	return true, nil
}

func (mq memoryQuery) neededFields(debug func(string, ...interface{})) []string {
	neededFields := make(map[string]bool)
	for field := range mq.filters {
		neededFields[field] = true
	}

	for field := range mq.project {
		debug("    project %v", field)
		neededFields[field] = true
	}

	for _, field := range mq.orderFields() {
		neededFields[field.field] = true
	}

	fields := make([]string, 0, len(neededFields))
	for f := range neededFields {
		fields = append(fields, f)
	}

	sort.Sort(sort.StringSlice(fields))
	debug("    needed fields: %+v", fields)
	return fields
}

func (mq *memoryQuery) checkIndexes(trace bool) error {
	debugMsgs := []string{}
	debug := func(format string, vars ...interface{}) {
		debugMsgs = append(debugMsgs, fmt.Sprintf(format, vars...))
		if trace {
			fmt.Printf(format+"\n", vars...)
		}
	}

	debug("looking for index for kind %s", mq.kind)

	if needs, err := mq.needsIndex(debug); err != nil {
		return err
	} else if !needs {
		return nil
	}

	orderFields := mq.orderFields()
	equalityFields := mq.equalityFields()
	inequalityField, err := mq.inequalityField()
	if err != nil {
		return err
	}

	neededFields := mq.neededFields(debug)

	if len(orderFields) > 0 {
		debug("    order %v", orderFields)
	}

	if mq.ancestor != nil {
		debug("    ancestor: true")
	}

	if inequalityField != "" {
		debug("    inequality %s", inequalityField)

		if len(orderFields) > 0 && inequalityField != orderFields[0].field {
			debug("    inequality field property and first sort order must be the same")
			return fmt.Errorf("invalid query: %s\n", strings.Join(debugMsgs, "\n"))
		}
	}

	for _, index := range mq.localDs.index[mq.kind] {
		debug("    considering %s", index)
		if len(neededFields) > len(index.fields) {
			debug("       too short")
			continue
		} else if len(neededFields) < len(index.fields) {
			debug("       too long") // no subindexes!
			continue
		} else if mq.ancestor != nil && !index.ancestor {
			debug("       no ancestor")
			continue
		} else if mq.ancestor == nil && index.ancestor {
			debug("       needs ancestor")
			continue
		}

		indexFields := make([]string, len(index.fields), len(index.fields))
		for fieldName, fieldData := range index.fields {
			indexFields[fieldData.index] = fieldName
		}

		matches := true

		indexEqualityFields := indexFields[:len(equalityFields)]
		debug("        eq fields: %v", indexEqualityFields)
		for _, field := range equalityFields {
			hasField := false
			for _, indexField := range indexEqualityFields {
				if field == indexField {
					hasField = true
				}
			}
			if !hasField {
				matches = false
				debug("        eq fields in wrong order: %s", field)
				break
			}
		}

		indexInequalityField := indexFields[len(equalityFields)]
		if inequalityField != "" && indexInequalityField != inequalityField {
			debug("        ineq fields in wrong order: %s", inequalityField)
			matches = false
		}

		indexOrderFields := indexFields[len(equalityFields):]
		for _, field := range orderFields {
			hasField := false
			for _, indexField := range indexOrderFields {
				if field.field == indexField {
					hasField = true
				}
			}
			if !hasField {
				matches = false
				debug("        order fields in wrong order: %s", field)
				break
			}
			if field.isDescending != index.fields[field.field].descending {
				matches = false
				debug("       wrong descencion on %s", field.field)
				break
			}
		}

		if !matches {
			debug("        field mismatch")
			continue
		}

		fieldIndexes := make([]int, len(neededFields))
		for i := range neededFields {
			if field, exists := index.fields[neededFields[i]]; !exists {
				debug("        field %s not indexed", neededFields[i])
				matches = false
				break
			} else {
				fieldIndexes[i] = field.index
			}
		}

		if !matches {
			debug("        field mismatch")
			continue
		}

		sort.Sort(sort.IntSlice(fieldIndexes)) // these should all be in a row
		for i, val := range fieldIndexes {
			if i != val {
				debug("        fields all present, but not in the right order")
				matches = false
				break
			}
		}

		if !matches {
			debug("        field mismatch")
			continue
		}
		debug("        matched")

		return nil
	}

	return fmt.Errorf("missing index: %s\n", strings.Join(debugMsgs, "\n"))
}

func (mq *memoryQuery) Ancestor(ancestor *DatastoreKey) DatastoreQuery {
	n := *mq
	if ancestor == nil {
		panic("datastore: nil query ancestor")
	}
	n.ancestor = ancestor
	return &n
}

func (mq *memoryQuery) Filter(how string, what interface{}) DatastoreQuery {
	n := *mq

	// Copy filters map
	n.filters = make(map[string]filter)
	for f, filter := range mq.filters {
		n.filters[f] = filter
	}

	// Extract field/op from how
	howS := strings.SplitN(how, " ", 2)
	field, op := howS[0], howS[1]

	// Add op/what to the corresponding filter (clone the filter to prevent clobbering the old one)
	f := n.filters[field].clone()
	f.add(op, what)
	n.filters[field] = f
	return &n
}

func (mq *memoryQuery) KeysOnly() DatastoreQuery {
	n := *mq
	n.keysOnly = true
	return &n
}

func (mq *memoryQuery) Limit(i int) DatastoreQuery {
	n := *mq
	n.limit = i
	return &n
}

func (mq *memoryQuery) Offset(i int) DatastoreQuery {
	n := *mq
	n.offset = i
	return &n
}

func (mq *memoryQuery) Order(how string) DatastoreQuery {
	n := *mq
	n.order = append(n.order, how)
	mq.order = mq.order[:len(mq.order):len(mq.order)] // cap mq.order so a later append doesn't clobber n.order up
	return &n
}

func (mq *memoryQuery) Project(fieldName ...string) DatastoreQuery {
	n := *mq

	n.project = make(map[string]bool)
	for _, name := range fieldName {
		n.project[name] = true
	}

	return &n
}

func (mq *memoryQuery) Distinct() DatastoreQuery {
	if mq.project == nil {
		panic("Distinct is only allowed with Projection Queries")
	}

	n := *mq
	n.distinct = true
	return &n
}

func (mq *memoryQuery) Start(c DatastoreCursor) DatastoreQuery {
	n := *mq
	n.start = c.(memoryCursor)
	return &n
}

func (mq *memoryQuery) Run() DatastoreIterator {
	//fmt.Printf("QUERY: %+v\n", mq)
	//for i := range items {
	//fmt.Printf("\t%d: %s: %+v\n", i, items[i].key, items[i].props)
	//}

	if items, indexErr := mq.getMatchingItems(); indexErr != nil {
		panic(indexErr)
	} else {
		return &memQueryIterator{items: items, keysOnly: mq.keysOnly, project: mq.project, addEntityFields: mq.addEntityFields}
	}
}

func (mq *memoryQuery) GetAll(dst interface{}) ([]*DatastoreKey, error) {
	items, indexErr := mq.getMatchingItems()
	if indexErr != nil {
		return nil, indexErr
	}

	keys := make([]*DatastoreKey, len(items))
	for i := range items {
		k := *items[i].key
		keys[i] = &k
	}

	if !mq.keysOnly {
		// underlying type we need -- dst is a pointer to an array or structs
		resultSlice := reflect.MakeSlice(reflect.TypeOf(dst).Elem(), len(items), len(items))
		for i := range items {
			if err := items[i].cp(resultSlice.Index(i).Addr().Interface(), mq.project, mq.addEntityFields); err != nil {
				return nil, err
			}

			k := *items[i].key
			keys[i] = &k
		}

		reflect.ValueOf(dst).Elem().Set(resultSlice)
	}

	return keys, nil
}

func (mq *memoryQuery) getMatchingItems() ([]*dsItem, error) {
	if err := mq.checkIndexes(false); err != nil {
		return nil, err
	}

	indexedFields := make(map[string]bool)
	for field := range mq.filters {
		indexedFields[field] = true
	}

	for _, order := range mq.order {
		if order[0] == '-' {
			indexedFields[order[1:]] = true
		} else {
			indexedFields[order] = true
		}
	}

	//fmt.Printf("MATCHING %+v\n", mq)
	items := make([]*dsItem, 0)

	mq.localDs.mtx.Lock()
	defer mq.localDs.mtx.Unlock()

	distinctHashes := make(map[string]bool)
	for _, item := range mq.localDs.entities {
		if mq.kind != KeyKind(item.key) {
			continue
		}

		if mq.ancestor != nil {
			k := item.key
			for k != nil {
				if k.Equal(mq.ancestor) {
					break
				}
				k = KeyParent(k)
			}

			if k == nil {
				continue
			}
		}

		skip := false
		for field, filter := range mq.filters {
			if !filter.cmp(field, item) {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		var dk distinctKeys

		for i := range item.props {
			if !item.props[i].NoIndex {
			} else if _, needIndex := indexedFields[item.props[i].Name]; needIndex {
				// not indexed
				skip = true
				break
			}
			if mq.distinct && mq.project[item.props[i].Name] {
				dk = append(dk, distinctKey{field: item.props[i].Name, value: item.props[i].Value})
			}
		}

		if skip {
			continue
		}

		if mq.distinct {
			if len(dk) == 0 {
				panic("Distinct() used in query, but no projection keys used")
			}
			itemhash := dk.Hash()
			if distinctHashes[itemhash] {
				continue
			}
			distinctHashes[itemhash] = true
		}

		items = append(items, item)
	}

	order := mq.order
	order = append(order, "__key__")

	sort.Sort(dsItemListSorter{items, order})

	if mq.start != nil && mq.start != firstItemCursor {
		i := 0
		for ; i < len(items); i++ {
			if items[i].key.Equal(mq.start) {
				break
			}
		}

		if i < len(items) {
			// cursor points to the last sent
			items = items[i+1:]
		} else {
			items = items[0:0]
		}
	}

	if mq.offset > 0 {
		items = items[mq.offset:]
	}

	if mq.limit > 0 && len(items) > mq.limit {
		items = items[0:mq.limit]
	}

	return items, nil
}

type memQueryIterator struct {
	items           []*dsItem
	next            int
	keysOnly        bool
	project         map[string]bool
	addEntityFields bool
}

func (mqi *memQueryIterator) Next(itemPtr interface{}) (*DatastoreKey, error) {
	if mqi.next >= len(mqi.items) {
		return nil, DatastoreDone
	}

	i := mqi.next
	mqi.next++

	if !mqi.keysOnly {
		if err := mqi.items[i].cp(itemPtr, mqi.project, mqi.addEntityFields); err != nil {
			return nil, err
		}
	}

	return mqi.items[i].key, nil
}

// this implementation probably isn't great because it doesn't handle the cursor record disappearing
func (mqi *memQueryIterator) Cursor() (DatastoreCursor, error) {
	if mqi.next == 0 {
		return firstItemCursor, nil
	}

	return memoryCursor(mqi.items[mqi.next-1].key), nil
}
