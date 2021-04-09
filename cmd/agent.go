package main

import (
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/contiv/libovsdb"
	"github.com/contiv/ofnet"
	"github.com/contiv/ofnet/ovsdbDriver"
)

const (
	RPC_PORT     = 20000
	OVSCTRL_PORT = 20001
	MASTER_PORT  = 30000
)
const LOCAL_IP = "10.10.1.1"
const VLAN_ID = 1000
const (
	localMac1 = "00:00:22:22:22:21"
	localMac2 = "00:00:22:22:22:22"
	localIp1  = "10.10.1.11"
	localIp2  = "10.10.1.12"
)

func main() {
	bridgeName := "vlanArpLearnerBridge"
	driver := ovsdbDriver.NewOvsDriver(bridgeName)

	// port1name := "p1"
	// driver.CreatePort(port1name, "internal", 0)

	localIp := net.ParseIP(LOCAL_IP)
	ofPortIpAddressUpdateMonitorChan := make(chan map[uint32][]net.IP, 1024)

	var uplinkPort ofnet.PortInfo
	uplinkPortName := "uplinkport"
	uplinkPortInterface0Name := "ens10"
	uplinkPortInterface0OfPort := uint32(15)
	uplinkPortInterface1Name := "ens11"
	uplinkPortInterface1OfPort := uint32(16)
	// PortType := "individual"
	PortType := "bond"

	link0 := ofnet.LinkInfo{
		Name:       uplinkPortInterface0Name,
		OfPort:     uplinkPortInterface0OfPort,
		LinkStatus: 0,
		Port:       &uplinkPort,
	}
	link1 := ofnet.LinkInfo{
		Name:       uplinkPortInterface1Name,
		OfPort:     uplinkPortInterface1OfPort,
		LinkStatus: 0,
		Port:       &uplinkPort,
	}
	uplinkPort = ofnet.PortInfo{
		Name:       uplinkPortName,
		Type:       PortType,
		LinkStatus: 0,
		MbrLinks:   []*ofnet.LinkInfo{&link0, &link1},
	}

	agent, err := ofnet.NewOfnetAgent(bridgeName, "vlanArpLearner", localIp, RPC_PORT, OVSCTRL_PORT, nil, nil, ofPortIpAddressUpdateMonitorChan)
	if err != nil {
		log.Fatalf("Error when init vlanAgent: %s", bridgeName)
	}
	// go mockAgentMonitor(ofPortIpAddressUpdateMonitorChan)

	err = driver.AddController("127.0.0.1", OVSCTRL_PORT)
	if err != nil {
		log.Fatalf("error when bridge %s connect to controller", bridgeName)
	}

	// Initialize default flow, TODO

	time.Sleep(300 * time.Millisecond)
	for i := 0; i < 30; i++ {
		time.Sleep(500 * time.Millisecond)
		if agent.IsSwitchConnected() {
			fmt.Println("############### connected to ofSwitch")
			break
		}
	}

	ovsClient, err := libovsdb.ConnectUnix("/usr/local/var/run/openvswitch/db.sock")
	if err != nil {
		log.Fatalf("error when init ovsdbEventHandler ovsClient: %v", err)
	}
	// value := reflect.ValueOf(driver).FieldByName("ovsClient")
	// fmt.Println(value)
	// ovsdbEventHandler := ofnet.NewOvsdbEventHandler(value.Interface().(*libovsdb.OvsdbClient))

	ovsdbEventHandler := ofnet.NewOvsdbEventHandler(ovsClient)
	ovsdbEventHandler.RegisterOvsdbEventCallbackHandlerFunc(ofnet.OvsdbEventHandlerFuncs{
		LocalEndpointAddFunc:        agent.GetDatapath().AddLocalEndpoint,
		LocalEndpointDeleteFunc:     agent.GetDatapath().RemoveLocalEndpoint,
		UplinkActiveSlaveUpdateFunc: agent.GetDatapath().UpdateUplink,
		UplinkAddFunc:               agent.AddUplink,
		UplinkDelFunc:               agent.RemoveUplink,
	})

	err = ovsdbEventHandler.StartOvsdbEventHandler()
	if err != nil {
		log.Fatalf("error when start ovsdbEventHandler: %v", err)
	}

	// Add policyRule
	rule1 := &ofnet.OfnetPolicyRule{
		RuleId:     "rule1",
		IpProtocol: uint8(1),
		SrcIpAddr:  "10.100.100.1",
		DstIpAddr:  "10.100.100.2",
		Action:     "allow",
	}

	rule2 := &ofnet.OfnetPolicyRule{
		RuleId:     "rule2",
		IpProtocol: uint8(1),
		DstIpAddr:  "10.100.100.3",
		Action:     "deny",
	}

	agent.GetDatapath().GetPolicyAgent().AddRuleToTier(rule1, ofnet.POLICY_DIRECTION_IN, ofnet.POLICY_TIER0)
	agent.GetDatapath().GetPolicyAgent().AddRuleToTier(rule2, ofnet.POLICY_DIRECTION_OUT, ofnet.POLICY_TIER1)

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func mockAgentMonitor(ofPortIpAddressUpdateMonitorChan chan map[uint32][]net.IP) {
	ticker := time.NewTicker(time.Second * 2)
	for _ = range ticker.C {
		select {
		case localEndpointInfo := <-ofPortIpAddressUpdateMonitorChan:
			fmt.Println(localEndpointInfo)
		default:
			fmt.Println("no ofPort update event")
		}
	}
}
