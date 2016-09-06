// +build appengine appenginevm

package appwrap

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

type dsItem struct {
	props datastore.PropertyList
	key   *datastore.Key
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
	case *datastore.Key:
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
		} else if aI.(bool) == false {
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
	case *datastore.Key:
		a, b := aI.(*datastore.Key), bI.(*datastore.Key)
		if a == b {
			return 0
		} else if a == (*datastore.Key)(nil) {
			return -1
		} else if b == (*datastore.Key)(nil) {
			return 1
		} else if a.IntID() < b.IntID() || a.StringID() < b.StringID() {
			return -1
		} else if a.IntID() > b.IntID() || a.StringID() > b.StringID() {
			return 1
		} else {
			return 0
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

func (item *dsItem) cp(dst interface{}, fields map[string]bool) error {
	props := item.props
	if fields != nil {
		props := make([]datastore.Property, 0, len(fields))
		for _, prop := range item.props {
			if fields[prop.Name] {
				props = append(props, prop)
			}
		}
	}

	//fmt.Printf("%T <- %+v (%d)\n", dst, props, len(item.props))
	if loadSaver, okay := dst.(datastore.PropertyLoadSaver); okay {
		//fmt.Printf("\tload saver\n")
		//fmt.Printf("FILLED %+v\n", dst)
		// Load() may mess with the array; don't let it break our stored data
		propsCopy := make([]datastore.Property, len(props))
		copy(propsCopy, props)
		return loadSaver.Load(propsCopy)
	} else {
		return datastore.LoadStruct(dst, props)
	}
}

type LocalDatastore struct {
	lastId       int64
	entities     map[string]*dsItem
	emptyContext context.Context
	mtx          *sync.Mutex
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

func NewLocalDatastore() Datastore {
	return &LocalDatastore{
		lastId:       1 << 30,
		entities:     make(map[string]*dsItem),
		emptyContext: StubContext(),
		mtx:          &sync.Mutex{},
	}
}

func (ds *LocalDatastore) Deadline(t time.Time) Datastore {
	return ds
}

func (ds *LocalDatastore) Namespace(ns string) Datastore {
	// this should do something?
	return ds
}

func (ds *LocalDatastore) AllocateIDs(kind string, parent *datastore.Key, n int) (int64, int64, error) {
	first := ds.lastId
	ds.lastId += int64(n)
	return first, first + int64(n) - 1, nil
}

func (ds *LocalDatastore) delete(keyStr string) {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()
	delete(ds.entities, keyStr)
}

func (ds *LocalDatastore) DeleteMulti(keys []*datastore.Key) (err error) {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()

	multiError := make(appengine.MultiError, len(keys))
	errors := false
	for i, k := range keys {
		if _, exists := ds.entities[k.String()]; !exists {
			multiError[i] = datastore.ErrNoSuchEntity
			errors = true
		} else {
			delete(ds.entities, k.String())
		}
	}

	if errors {
		err = multiError
	}

	return
}

func (ds *LocalDatastore) get(keyStr string) (item *dsItem, found bool) {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()
	item, found = ds.entities[keyStr]
	return
}

func (ds *LocalDatastore) Get(k *datastore.Key, dst interface{}) error {
	if item, exists := ds.get(k.String()); !exists {
		return datastore.ErrNoSuchEntity
	} else {
		return item.cp(dst, nil)
	}
}

func (ds *LocalDatastore) GetMulti(keys []*datastore.Key, dstIntf interface{}) error {
	dstValue := reflect.ValueOf(dstIntf)
	errors := false
	multiError := make(appengine.MultiError, len(keys))
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

func (ds *LocalDatastore) NewKey(kind string, sId string, iId int64, parent *datastore.Key) *datastore.Key {
	return datastore.NewKey(ds.emptyContext, kind, sId, iId, parent)
}

func (ds *LocalDatastore) put(keyStr string, item *dsItem) {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()
	ds.entities[keyStr] = item
}

func (ds *LocalDatastore) Put(key *datastore.Key, src interface{}) (*datastore.Key, error) {
	finalKeyCopy := *key
	finalKey := &finalKeyCopy

	if finalKey.StringID() == "" && finalKey.IntID() == 0 {
		finalKey = ds.NewKey(finalKey.Kind(), "", ds.lastId, finalKey.Parent())
		ds.lastId++
	}

	if saver, okay := src.(datastore.PropertyLoadSaver); okay {
		if item, err := saver.Save(); err != nil {
			return nil, err
		} else {
			k := *finalKey
			ds.put(finalKey.String(), &dsItem{props: item, key: &k})
		}
	} else if item, err := datastore.SaveStruct(src); err != nil {
		return nil, err
	} else {
		k := *finalKey
		ds.put(finalKey.String(), &dsItem{props: item, key: &k})
	}

	return finalKey, nil
}

func (ds *LocalDatastore) PutMulti(keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	srcValue := reflect.ValueOf(src)
	finalKeys := make([]*datastore.Key, len(keys))
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

func (ds *LocalDatastore) RunInTransaction(f func(coreds Datastore) error, opts *datastore.TransactionOptions) error {
	dsCopy := *ds
	err := f(&dsCopy)
	*ds = dsCopy
	return err
}

func (ds *LocalDatastore) NewQuery(kind string) DatastoreQuery {
	return &memoryQuery{localDs: ds, kind: kind}
}

type filter struct {
	field string
	op    string
	val   interface{}
}

func (f filter) cmpSingleton(val interface{}) bool {
	c := cmp(val, f.val)

	switch f.op {
	case "=":
		return c == 0
	case "<":
		return c == -1
	case ">":
		return c == 1
	case "<=":
		return c != 1
	case ">=":
		return c != -1
	}

	panic(fmt.Sprintf("bad operator %s for filter", f.op))
}

func (f filter) cmp(item *dsItem) bool {
	if f.field == "__key__" {
		return f.cmpSingleton(item.key)
	}

	for x := range item.props {
		if item.props[x].Name == f.field {
			if f.cmpSingleton(item.props[x].Value) {
				return true
			}
		}
	}

	return false
}

type memoryCursor *datastore.Key

var firstItemCursor memoryCursor = &datastore.Key{}

type memoryQuery struct {
	localDs  *LocalDatastore
	filters  []filter
	kind     string
	ancestor *datastore.Key
	keysOnly bool
	limit    int
	offset   int
	start    memoryCursor
	order    []string
	project  map[string]bool
}

func (mq *memoryQuery) Ancestor(ancestor *datastore.Key) DatastoreQuery {
	n := *mq
	n.ancestor = ancestor
	return &n
}

func (mq *memoryQuery) Filter(how string, what interface{}) DatastoreQuery {
	filter := filter{
		field: strings.SplitN(how, " ", 2)[0],
		op:    strings.SplitN(how, " ", 2)[1],
		val:   what,
	}

	n := *mq
	n.filters = append(n.filters, filter)
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

func (mq *memoryQuery) Start(c DatastoreCursor) DatastoreQuery {
	n := *mq
	n.start = c.(memoryCursor)
	return &n
}

func (mq *memoryQuery) Run() DatastoreIterator {
	items := mq.getMatchingItems()
	//fmt.Printf("QUERY: %+v\n", mq)
	//for i := range items {
	//fmt.Printf("\t%d: %s: %+v\n", i, items[i].key, items[i].props)
	//}

	return &memQueryIterator{items: items, keysOnly: mq.keysOnly, project: mq.project}
}

func (mq *memoryQuery) GetAll(dst interface{}) ([]*datastore.Key, error) {
	items := mq.getMatchingItems()

	keys := make([]*datastore.Key, len(items))
	for i := range items {
		k := *items[i].key
		keys[i] = &k
	}

	if !mq.keysOnly {
		// underlying type we need -- dst is a pointer to an array or structs
		resultSlice := reflect.MakeSlice(reflect.TypeOf(dst).Elem(), len(items), len(items))
		for i := range items {
			if err := items[i].cp(resultSlice.Index(i).Addr().Interface(), mq.project); err != nil {
				return nil, err
			}

			k := *items[i].key
			keys[i] = &k
		}

		reflect.ValueOf(dst).Elem().Set(resultSlice)
	}

	return keys, nil
}

func (mq *memoryQuery) getMatchingItems() []*dsItem {
	indexedFields := make(map[string]bool)
	for _, filter := range mq.filters {
		indexedFields[filter.field] = true
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

	for _, item := range mq.localDs.entities {
		if mq.kind != item.key.Kind() {
			continue
		}

		if mq.ancestor != nil {
			k := item.key
			for k != nil {
				if k.Equal(mq.ancestor) {
					break
				}
				k = k.Parent()
			}

			if k == nil {
				continue
			}
		}

		skip := false
		for _, filter := range mq.filters {
			//t := filter.cmp(item)
			//fmt.Printf("HERE %+v %+v %+v: %d\n", filter, item, filter.val, t)

			if !filter.cmp(item) {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		for i := range item.props {
			if !item.props[i].NoIndex {
			} else if _, needIndex := indexedFields[item.props[i].Name]; needIndex {
				// not indexed
				skip = true
				break
			}
		}

		if skip {
			continue
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

	//fmt.Printf("QUERY: %+v\n", mq)
	//for i := range items {
	//fmt.Printf("\t%d: %s: %+v\n", i, items[i].key, items[i].props)
	//}

	return items
}

type memQueryIterator struct {
	items    []*dsItem
	next     int
	keysOnly bool
	project  map[string]bool
}

func (mqi *memQueryIterator) Next(itemPtr interface{}) (*datastore.Key, error) {
	if mqi.next >= len(mqi.items) {
		return nil, datastore.Done
	}

	i := mqi.next
	mqi.next++

	if !mqi.keysOnly {
		if err := mqi.items[i].cp(itemPtr, mqi.project); err != nil {
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
