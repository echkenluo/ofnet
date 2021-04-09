package ofnet

import (
	"fmt"
	"reflect"
	"sync"

	log "github.com/Sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/contiv/libovsdb"
)

const (
	LocalEndpointIdentity = "attached-mac"
	UplinkPortIdentity    = "uplink-port"
)

type EventHandler interface {
	AddLocalEndpoint(endpoint OfnetEndpoint) error
	RemoveLocalEndPoint(endpoint OfnetEndpoint) error
	UpdateUplinkActiveSlave(ofPort uint32) error
	AddUplink(port *PortInfo) error
	DeleteUplink(portName string) error
}

// type ovsdbEventHandlerFunc func(eventParams interface{})
type OvsdbEventHandlerFuncs struct {
	LocalEndpointAddFunc        func(endpoint OfnetEndpoint) error
	LocalEndpointDeleteFunc     func(endpoint OfnetEndpoint) error
	UplinkActiveSlaveUpdateFunc func(portName string, updates PortUpdates) error
	UplinkAddFunc               func(port *PortInfo) error
	UplinkDelFunc               func(portName string) error
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

func (o OvsdbEventHandlerFuncs) UpdateUplinkActiveSlave(ofPort uint32) error {
	if o.UplinkActiveSlaveUpdateFunc != nil {
		portUpdate := PortUpdates{
			Updates: []PortUpdate{{
				UpdateInfo: ofPort,
			}},
		}
		return o.UplinkActiveSlaveUpdateFunc("", portUpdate)
	}

	return nil
}

func (o OvsdbEventHandlerFuncs) AddUplink(port *PortInfo) error {
	if o.UplinkAddFunc != nil {
		return o.UplinkAddFunc(port)
	}

	return nil
}

func (o OvsdbEventHandlerFuncs) DeleteUplink(portName string) error {
	if o.UplinkDelFunc != nil {
		return o.UplinkDelFunc(portName)
	}

	return nil
}

type ovsdbEventHandler struct {
	cacheLock   sync.RWMutex
	ovsdbCache  map[string]map[string]libovsdb.Row
	ovsdbClient *libovsdb.OvsdbClient

	ovsdbEventHandlerFuncs           OvsdbEventHandlerFuncs
	localEndpointHardWareAddrCache   sets.String
	activeSlaveInterfaceHardwareAddr string
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
		"Port":      {Select: selectAll, Columns: []string{"name", "interfaces", "bond_active_slave", "external_ids"}},
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

func (o *ovsdbEventHandler) filterUplinkDeleted(rowupdate libovsdb.RowUpdate) *string {
	if rowupdate.Old.Fields["external_ids"] != nil {
		oldExternalIds := rowupdate.Old.Fields["external_ids"].(libovsdb.OvsMap).GoMap
		if _, ok := oldExternalIds[UplinkPortIdentity]; ok {
			uplinkPortName := rowupdate.Old.Fields["name"].(string)

			return &uplinkPortName
		}
	}

	return nil
}

func (o *ovsdbEventHandler) filterUplinkAdded(rowupdate libovsdb.RowUpdate) *PortInfo {
	if rowupdate.New.Fields["external_ids"] != nil {
		newExternalIds := rowupdate.New.Fields["external_ids"].(libovsdb.OvsMap).GoMap
		if _, ok := newExternalIds[UplinkPortIdentity]; ok {
			if rowupdate.Old.Fields["external_ids"] != nil {
				oldExternalIds := rowupdate.Old.Fields["external_ids"].(libovsdb.OvsMap).GoMap
				if _, ok := oldExternalIds[UplinkPortIdentity]; ok {
					// UplinkPortIdentity already exists
					return nil
				}

				uplinkInterfaceUUIds := listUUID(rowupdate.New.Fields["interfaces"])
				uplinkPortName := rowupdate.New.Fields["name"].(string)
				var portInfo *PortInfo
				var mbrLinkInfo []*LinkInfo
				var portType string = "individual"
				if len(uplinkInterfaceUUIds) != 1 {
					portType = "bond"
				}

				for _, interfaceUUId := range uplinkInterfaceUUIds {
					var interfaceOfPort uint32
					uplinkInterface := o.ovsdbCache["Interface"][interfaceUUId.GoUuid]

					interfaceName, _ := uplinkInterface.Fields["name"].(string)
					ofport, ok := uplinkInterface.Fields["ofport"].(float64)
					if ok && ofport > 0 {
						interfaceOfPort = uint32(ofport)
					}

					mbrLinkInfo = append(mbrLinkInfo, &LinkInfo{
						Name:       interfaceName,
						OfPort:     interfaceOfPort,
						LinkStatus: 0,
						Port:       portInfo,
					})
				}

				portInfo = &PortInfo{
					Name:       uplinkPortName,
					Type:       portType,
					LinkStatus: 0,
					MbrLinks:   mbrLinkInfo,
				}

				return portInfo
			}
		}
	}

	return nil
}

func (o *ovsdbEventHandler) processUplinkAdd(rowupdate libovsdb.RowUpdate) {
	addedPortInfo := o.filterUplinkAdded(rowupdate)
	if addedPortInfo != nil {
		log.Infof("addedPort: %+v", addedPortInfo)
		o.ovsdbEventHandlerFuncs.AddUplink(addedPortInfo)
	}
}

func (o *ovsdbEventHandler) processUplinkDelete(rowupdate libovsdb.RowUpdate) {
	deletedPortName := o.filterUplinkDeleted(rowupdate)
	if deletedPortName != nil {
		log.Infof("deletedPortName: %+v", deletedPortName)
		o.ovsdbEventHandlerFuncs.DeleteUplink(*deletedPortName)
	}
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

func (o *ovsdbEventHandler) filterCurrentBondActiveSlave(rowupdate libovsdb.RowUpdate) *uint32 {
	var activeInterfaceOfPort uint32

	curActiveSlaveMacAddr, _ := rowupdate.New.Fields["bond_active_slave"].(string)
	// fmt.Printf("curActiveSlaveMacAddr: %+v, preActiveSlaveMacAddr: %+v\n", curActiveSlaveMacAddr, o.activeSlaveInterfaceHardwareAddr)
	if curActiveSlaveMacAddr == "" {
		return nil
	}

	// bondInterfaceUUIds, _ := rowupdate.New.Fields["interfaces"].([]string)
	bondInterfaceUUIds := listUUID(rowupdate.New.Fields["interfaces"])
	// fmt.Printf("bondInterfaceUUIds: %+v, converted bondInterfaceUUIds: %+v\n", rowupdate.New.Fields["interfaces"], bondInterfaceUUIds)

	if curActiveSlaveMacAddr != o.activeSlaveInterfaceHardwareAddr {
		for _, interfaceUUId := range bondInterfaceUUIds {
			ovsInterface, ok := o.ovsdbCache["Interface"][interfaceUUId.GoUuid]
			if !ok {
				log.Infof("Failed to get ovs interface: %+v", interfaceUUId)
				continue
			}

			interfaceMac, _ := ovsInterface.Fields["mac_in_use"].(string)
			if interfaceMac == curActiveSlaveMacAddr {
				ofPort, ok := ovsInterface.Fields["ofport"].(float64)
				if ok && ofPort > 0 {
					activeInterfaceOfPort = uint32(ofPort)
				}
				o.activeSlaveInterfaceHardwareAddr = curActiveSlaveMacAddr

				return &activeInterfaceOfPort
			}
		}
	}

	return nil
}

func (o *ovsdbEventHandler) processBondActiveSlaveSwitch(rowupdate libovsdb.RowUpdate) {
	activeInterfaceOfPort := o.filterCurrentBondActiveSlave(rowupdate)

	if activeInterfaceOfPort != nil {
		// fmt.Printf("Switch bond active slave to: %+v\n", *activeInterfaceOfPort)
		o.ovsdbEventHandlerFuncs.UpdateUplinkActiveSlave(*activeInterfaceOfPort)
	}
}

func listUUID(uuidList interface{}) []libovsdb.UUID {
	var idList []libovsdb.UUID

	switch uuidList.(type) {
	case libovsdb.UUID:
		return []libovsdb.UUID{uuidList.(libovsdb.UUID)}
	case libovsdb.OvsSet:
		uuidSet := uuidList.(libovsdb.OvsSet).GoSet
		for item := range uuidSet {
			idList = append(idList, listUUID(uuidSet[item])...)
		}
	}

	return idList
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
				if table == "Interface" {
					o.processEndpointAdd(row)
				}

				if table == "Port" {
					fmt.Printf("Table port update\n")
					o.processBondActiveSlaveSwitch(row)
					o.processUplinkAdd(row)
				}

				o.ovsdbCache[table][uuid] = row.New
			} else {
				if table == "Interface" {
					o.processEndpointDel(row)
				}

				if table == "Port" {
					o.processUplinkDelete(row)
				}

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
