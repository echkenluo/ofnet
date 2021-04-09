package ofnet

import (
	"fmt"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/contiv/libovsdb"
)

type EventHandler interface {
	AddLocalEndpoint(endpoint OfnetEndpoint) error
	RemoveLocalEndPoint(endpoint OfnetEndpoint) error
}

// type ovsdbEventHandlerFunc func(eventParams interface{})
type OvsdbEventHandlerFuncs struct {
	LocalEndpointAddFunc    func(endpoint OfnetEndpoint) error
	LocalEndpointDeleteFunc func(endpoint OfnetEndpoint) error
}

func (o *ovsdbEventHandler) RegisterOvsdbEventCallbackHandlerFunc(ovsdbEventHandlerFuncs OvsdbEventHandlerFuncs) {
	o.ovsdbEventHandlerFuncs = ovsdbEventHandlerFuncs
}

func (o OvsdbEventHandlerFuncs) AddLocalEndpoint(endpoint OfnetEndpoint) error {
	if o.LocalEndpointAddFunc != nil {
		o.LocalEndpointAddFunc(endpoint)
	}

	return nil
}

func (o OvsdbEventHandlerFuncs) RemoveLocalEndPoint(endpoint OfnetEndpoint) error {
	if o.LocalEndpointDeleteFunc != nil {
		o.LocalEndpointDeleteFunc(endpoint)
	}

	return nil
}

type ovsdbEventHandler struct {
	cacheLock            sync.RWMutex
	ovsdbCache           map[string]map[string]libovsdb.Row
	ovsdbClient          *libovsdb.OvsdbClient

	ovsdbEventHandlerFuncs OvsdbEventHandlerFuncs
	localEndpointHardWareAddrCache sets.String
}

func NewOvsdbEventHandler(ovsdbClient *libovsdb.OvsdbClient) *ovsdbEventHandler {
	ovsdbCacheMap := make(map[string]map[string]libovsdb.Row)
	ovsdbEventHandler := &ovsdbEventHandler{
		ovsdbCache:  ovsdbCacheMap,
		ovsdbClient: ovsdbClient,
	}

	ovsdbEventHandler.localEndpointHardWareAddrCache = sets.NewString()

	return ovsdbEventHandler
}

func (o *ovsdbEventHandler) StartOvsdbEventHandler() error {
	o.ovsdbClient.Register(o)

	selectAll := libovsdb.MonitorSelect{
		Initial: true,
		Insert:  true,
		Delete:  true,
		Modify:  true,
	}
	requests := map[string]libovsdb.MonitorRequest{
		"Interface": {Select: selectAll, Columns: []string{"name", "mac_in_use", "ofport", "type", "external_ids"}},
	}

	initial, err := o.ovsdbClient.Monitor("Open_vSwitch", nil, requests)
	if err != nil {
		return fmt.Errorf("monitor ovsdb %s: %s", "Open_vSwitch", err)
	}

	o.Update(nil, *initial)

	return nil
}

func (o *ovsdbEventHandler) filterEndpointAdded(rowupdate libovsdb.RowUpdate) *OfnetEndpoint {
	if rowupdate.New.Fields["external_ids"] != nil {
		newExternalIds := rowupdate.New.Fields["external_ids"].(libovsdb.OvsMap).GoMap
		for newName, newValue := range newExternalIds {
			if newName.(string) == "attached-mac" {
				if o.localEndpointHardWareAddrCache.Has(newValue.(string)) {
					return nil
				}
				o.localEndpointHardWareAddrCache.Insert(newValue.(string))

				return &OfnetEndpoint{
					MacAddrStr: newValue.(string),
				}
			}
		}
	}

	return nil
}

func (o *ovsdbEventHandler) filterEndpoingDeleted(rowupdate libovsdb.RowUpdate) *OfnetEndpoint {
	if rowupdate.Old.Fields["external_ids"] != nil {
		oldExternalIds := rowupdate.Old.Fields["external_ids"].(libovsdb.OvsMap).GoMap
		for oldName, oldValue := range oldExternalIds {
			if oldName.(string) == "attached-mac" {
				o.localEndpointHardWareAddrCache.Delete(oldValue.(string))

				return &OfnetEndpoint{
					MacAddrStr: oldValue.(string),
				}
			}
		}
	}

	return nil
}

func (o *ovsdbEventHandler) processEndpointAdd(rowupdate libovsdb.RowUpdate) {
	addedEndpoints := o.filterEndpointAdded(rowupdate)
	if addedEndpoints != nil {
		o.ovsdbEventHandlerFuncs.AddLocalEndpoint(*addedEndpoints)
	}
}

func (o *ovsdbEventHandler) processEndpointDel(rowupdate libovsdb.RowUpdate) {
	deletedEndpoints := o.filterEndpoingDeleted(rowupdate)
	if deletedEndpoints != nil {
		o.ovsdbEventHandlerFuncs.RemoveLocalEndPoint(*deletedEndpoints)
	}
}

func (o *ovsdbEventHandler) Update(context interface{}, tableUpdates libovsdb.TableUpdates) {
	o.cacheLock.Lock()
	defer o.cacheLock.Unlock()

	for table, tableUpdate := range tableUpdates.Updates {
		if _, ok := o.ovsdbCache[table]; !ok {
			o.ovsdbCache[table] = make(map[string]libovsdb.Row)
		}
		for uuid, row := range tableUpdate.Rows {
			empty := libovsdb.Row{}
			if !reflect.DeepEqual(row.New, empty) {
				o.processEndpointAdd(row)

				o.ovsdbCache[table][uuid] = row.New
			} else {
				o.processEndpointDel(row)

				delete(o.ovsdbCache[table], uuid)
			}
		}
	}

}

func (o *ovsdbEventHandler) Locked(context []interface{}) {

}

func (o *ovsdbEventHandler) Stolen(context []interface{}) {
}

func (o *ovsdbEventHandler) Echo(context []interface{}) {
}
