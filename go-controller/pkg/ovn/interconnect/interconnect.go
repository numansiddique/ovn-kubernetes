package interconnect

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"sync"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"

	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/sbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	utilnet "k8s.io/utils/net"
)

const (
	transitSwitchTunnelKey = "16711683"
)

type nodeInfo struct {
	// the node's Name
	name string

	// Is the node part of global availability zone.
	isNodeGlobalAz bool

	tsMac net.HardwareAddr
	tsNet string
	tsIp  string
	id    int

	nodeSubnets []*net.IPNet
	joinSubnets []*net.IPNet
	chassisID   string
}

type Controller struct {
	sync.Mutex

	client clientset.Interface
	kube   kube.Interface

	// libovsdb northbound client interface
	nbClient      libovsdbclient.Client
	modelNbClient libovsdbops.ModelClient

	// libovsdb southbound client interface
	sbClient libovsdbclient.Client

	sharedInformer informers.SharedInformerFactory

	localAzName string

	// nodes is the list of nodes we know about
	// map of name -> info
	nodes map[string]nodeInfo

	localNodeInfo *nodeInfo

	tsBaseIp *big.Int
}

// NewInterconnect creates a new interconnect
func NewController(client clientset.Interface, kube kube.Interface, nbClient libovsdbclient.Client,
	sbClient libovsdbclient.Client) *Controller {
	sharedInformer := informers.NewSharedInformerFactory(client, 0)
	nodeInformer := sharedInformer.Core().V1().Nodes().Informer()
	icConnect := Controller{
		client:         client,
		kube:           kube,
		nbClient:       nbClient,
		modelNbClient:  libovsdbops.NewModelClient(nbClient),
		sbClient:       sbClient,
		sharedInformer: sharedInformer,
		nodes:          map[string]nodeInfo{},
		localAzName:    "",
		localNodeInfo:  nil,
		tsBaseIp:       utilnet.BigForIP(net.IPv4(169, 254, 0, 0)),
	}

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			klog.Infof("Interconnect: AddFunc for : %s", node.Name)
			if !ok {
				return
			}
			icConnect.updateNode(node)
		},
		UpdateFunc: func(old, new interface{}) {
			oldObj, ok := old.(*v1.Node)
			if !ok {
				return
			}
			newObj, ok := new.(*v1.Node)
			if !ok {
				return
			}
			klog.Infof("Interconnect: UpdateFunc for : %s", newObj.Name)

			// Make sure object was actually changed and not pending deletion
			if oldObj.GetResourceVersion() == newObj.GetResourceVersion() || !newObj.GetDeletionTimestamp().IsZero() {
				klog.Infof("Interconnect: UpdateFunc for : %s returning.. nothing changed", newObj.Name)
				return
			}

			icConnect.updateNode(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.Errorf("couldn't understand non-tombstone object")
					return
				}
				node, ok = tombstone.Obj.(*v1.Node)
				if !ok {
					klog.Errorf("couldn't understand tombstone object")
					return
				}
			}
			klog.Infof("Interconnect: DeleteFunc for : %s", node.Name)
			//icConnect.removeNode(node.Name)
		},
	})

	return &icConnect
}

func (ic *Controller) Run(stopCh <-chan struct{}) error {
	ic.updateLocalAzName()
	ic.sharedInformer.Start(stopCh)
	<-stopCh
	return nil
}

func (ic *Controller) updateLocalAzName() {
	ic.Lock()
	defer ic.Unlock()
	localAzName, err := libovsdbops.GetNBAvailbilityZoneName(ic.nbClient)
	if err == nil {
		ic.localAzName = localAzName
	}
}

func (ic *Controller) updateNode(node *v1.Node) {
	nodeId := util.GetNodeId(node)
	if nodeId == -1 {
		// Don't consider this node as master has not allocated node id yet.
		return
	}

	if ic.localAzName == "" {
		ic.updateLocalAzName()
	}

	ic.updateNodeInfo(node, nodeId)
	err := ic.createRemoteChassis(node)
	if err != nil {
		return
	}
	if ni, ok := ic.nodes[node.Name]; ok {
		ic.syncNodeResources(ni)
	}
}

/*
func (ic *Controller) removeNode(nodeName string) error {
	return nil
}
*/

func (ic *Controller) updateNodeInfo(node *v1.Node, nodeId int) {
	//TODO: generate mac address and IP properly.  Its a hack for now.
	tsIp := utilnet.AddIPOffset(ic.tsBaseIp, nodeId)
	tsMac := util.IPAddrToHWAddr(tsIp)

	chassisID, err := util.ParseNodeChassisIDAnnotation(node)
	if err != nil {
		klog.Infof("Failed to parse node chassis-id for node - %s", node.Name)
		return
	}

	nodeSubnets, err := util.ParseNodeHostSubnetAnnotation(node)
	if err != nil {
		klog.Infof("Failed to parse node %s subnets annotation %v", node.Name, err)
		return
	}

	joinSubnets, err := config.GetJoinSubnets(nodeId)
	if err != nil {
		klog.Infof("Failed to parse node %s join subnets annotation %v", node.Name, err)
		return
	}

	ni := nodeInfo{
		name:           node.Name,
		isNodeGlobalAz: util.IsNodeGlobalAz(node),
		id:             nodeId,
		tsMac:          tsMac,
		tsNet:          tsIp.String() + "/16",
		tsIp:           tsIp.String(),
		chassisID:      chassisID,
		nodeSubnets:    nodeSubnets,
		joinSubnets:    joinSubnets,
	}

	ic.Lock()
	if existing, ok := ic.nodes[node.Name]; ok {
		if reflect.DeepEqual(existing, ni) {
			ic.Unlock()
			return
		}
	}

	ic.nodes[node.Name] = ni
	ic.Unlock()
}

func (ic *Controller) createLocalAzNodeResources(ni nodeInfo) error {
	logicalSwitch := nbdb.LogicalSwitch{
		Name: types.TransitSwitch,
		OtherConfig: map[string]string{
			"interconn-ts":             types.TransitSwitch,
			"requested-tnl-key":        transitSwitchTunnelKey,
			"mcast_snoop":              "true",
			"mcast_flood_unregistered": "true",
		},
	}

	logicalRouterPortName := types.RouterToTransitSwitchPrefix + ni.name
	logicalRouterPort := nbdb.LogicalRouterPort{
		Name:     logicalRouterPortName,
		MAC:      ni.tsMac.String(),
		Networks: []string{ni.tsNet},
		Options: map[string]string{
			"mcast_flood": "true",
		},
	}
	logicalRouter := nbdb.LogicalRouter{}
	opModels := []libovsdbops.OperationModel{
		{
			Model: &logicalRouterPort,
			OnModelUpdates: []interface{}{
				&logicalRouterPort.Networks,
				&logicalRouterPort.MAC,
				&logicalRouterPort.Options,
			},
			DoAfter: func() {
				logicalRouter.Ports = []string{logicalRouterPort.UUID}
			},
		},
		{
			Model:          &logicalRouter,
			ModelPredicate: func(lr *nbdb.LogicalRouter) bool { return lr.Name == types.OVNClusterRouter },
			OnModelMutations: []interface{}{
				&logicalRouter.Ports,
			},
			ErrNotFound: true,
		},
		{
			Model:          &logicalSwitch,
			ModelPredicate: func(ls *nbdb.LogicalSwitch) bool { return ls.Name == types.TransitSwitch },
			OnModelUpdates: []interface{}{
				&logicalSwitch.OtherConfig,
			},
		},
	}
	if _, err := ic.modelNbClient.CreateOrUpdate(opModels...); err != nil {
		return fmt.Errorf("failed to add logical port to router, error: %v", err)
	}

	lspOptions := map[string]string{
		"router-port":       types.RouterToTransitSwitchPrefix + ni.name,
		"requested-tnl-key": strconv.Itoa(ni.id),
	}

	return ic.addNodeLogicalSwitchPort(types.TransitSwitch, types.TransitSwitchToRouterPrefix+ni.name,
		"router", []string{"router"}, lspOptions)
}

func (ic *Controller) addNodeLogicalSwitchPort(logicalSwitchName, portName, portType string, addresses []string, options map[string]string) error {
	logicalSwitch := nbdb.LogicalSwitch{
		Name: types.TransitSwitch,
		OtherConfig: map[string]string{
			"interconn-ts":      types.TransitSwitch,
			"requested-tnl-key": transitSwitchTunnelKey,
		},
	}

	logicalSwitchPort := nbdb.LogicalSwitchPort{
		Name:      portName,
		Type:      portType,
		Options:   options,
		Addresses: addresses,
	}
	opModels := []libovsdbops.OperationModel{
		{
			Model: &logicalSwitchPort,
			OnModelUpdates: []interface{}{
				&logicalSwitchPort.Addresses,
				&logicalSwitchPort.Type,
				&logicalSwitchPort.Options,
				&logicalSwitchPort.Addresses,
			},
			DoAfter: func() {
				logicalSwitch.Ports = []string{logicalSwitchPort.UUID}
			},
		},
		{
			Model:          &logicalSwitch,
			ModelPredicate: func(ls *nbdb.LogicalSwitch) bool { return ls.Name == logicalSwitchName },
			OnModelMutations: []interface{}{
				&logicalSwitch.Ports,
				&logicalSwitch.OtherConfig,
			},
		},
	}
	_, err := ic.modelNbClient.CreateOrUpdate(opModels...)
	if err != nil {
		return fmt.Errorf("failed to add logical port %s to switch %s, error: %v", portName, logicalSwitch.Name, err)
	}

	return nil
}

func (ic *Controller) createRemoteAzNodeResources(ni nodeInfo) error {
	remotePortAddr := ni.tsMac.String() + " " + ni.tsNet
	lspOptions := map[string]string{
		"requested-tnl-key": strconv.Itoa(ni.id),
	}
	err := ic.addNodeLogicalSwitchPort(types.TransitSwitch, types.TransitSwitchToRouterPrefix+ni.name, "remote", []string{remotePortAddr}, lspOptions)

	if err != nil {
		return err
	}
	// Set the port binding chassis.
	err = ic.SetRemotePortBindingChassis(types.TransitSwitchToRouterPrefix+ni.name, ni)
	if err != nil {
		return err
	}

	err = ic.addRemoteNodeStaticRoutes(ni)
	if err != nil {
		return err
	}

	logicalRouterPort := &nbdb.LogicalRouterPort{
		Name: types.RouterToTransitSwitchPrefix + ni.name,
	}
	ctx, cancel := context.WithTimeout(context.Background(), ovntypes.OVSDBTimeout)
	defer cancel()
	err = ic.nbClient.Get(ctx, logicalRouterPort)

	if err == nil {
		clusterRouterModel := nbdb.LogicalRouter{}
		opModels := []libovsdbops.OperationModel{
			{
				Model: logicalRouterPort,
				DoAfter: func() {
					if logicalRouterPort.UUID != "" {
						clusterRouterModel.Ports = []string{logicalRouterPort.UUID}
					}
				},
			},
			{
				Model:          &clusterRouterModel,
				ModelPredicate: func(lr *nbdb.LogicalRouter) bool { return lr.Name == types.OVNClusterRouter },
				OnModelMutations: []interface{}{
					&clusterRouterModel.Ports,
				},
			},
		}

		err = ic.modelNbClient.Delete(opModels...)
		if err != nil {
			return fmt.Errorf("failed to delete logical router port %s from router %s, error: %v", logicalRouterPort.Name, types.OVNClusterRouter, err)
		}
	}

	return nil
}

func (ic *Controller) addRemoteNodeStaticRoutes(ni nodeInfo) error {
	logicalRouter := nbdb.LogicalRouter{Name: types.OVNClusterRouter}
	opModels := []libovsdbops.OperationModel{}

	addRoute := func(subnet *net.IPNet) {
		logicalRouterStaticRoute := nbdb.LogicalRouterStaticRoute{
			ExternalIDs: map[string]string{
				"ic-node": ni.name,
			},
			Nexthop:  ni.tsIp,
			IPPrefix: subnet.String(),
		}
		opModels = append(opModels, []libovsdbops.OperationModel{
			{
				Model: &logicalRouterStaticRoute,
				ModelPredicate: func(lrsr *nbdb.LogicalRouterStaticRoute) bool {
					return lrsr.IPPrefix == subnet.String() &&
						lrsr.Nexthop == ni.tsIp &&
						lrsr.ExternalIDs["ic-node"] == ni.name
				},
				DoAfter: func() {
					if logicalRouterStaticRoute.UUID != "" {
						logicalRouter.StaticRoutes = []string{logicalRouterStaticRoute.UUID}
					}
				},
			}, {
				Model: &logicalRouter,
				ModelPredicate: func(lr *nbdb.LogicalRouter) bool {
					return lr.Name == types.OVNClusterRouter
				},
				OnModelMutations: []interface{}{
					&logicalRouter.StaticRoutes,
				},
				ErrNotFound: true,
			},
		}...)
	}

	for _, subnet := range ni.nodeSubnets {
		addRoute(subnet)
	}

	for _, subnet := range ni.joinSubnets {
		addRoute(subnet)
	}

	if _, err := ic.modelNbClient.CreateOrUpdate(opModels...); err != nil {
		return fmt.Errorf("unable to create static routes: %v", err)
	}

	return nil
}

func (ic *Controller) SetRemotePortBindingChassis(portName string, ni nodeInfo) error {
	remotePort := &sbdb.PortBinding{LogicalPort: portName}

	ctx, cancel := context.WithTimeout(context.Background(), ovntypes.OVSDBTimeout)
	defer cancel()
	err := ic.sbClient.Get(ctx, remotePort)
	if err != nil {
		return fmt.Errorf("failed to get remote port %s, error: %v", portName, err)
	}

	chassis := &sbdb.Chassis{
		Hostname: ni.name,
		Name:     ni.chassisID,
	}

	err = ic.sbClient.Get(ctx, chassis)
	if err != nil {
		return fmt.Errorf("failed to get remote chassis id %s, error: %v", portName, err)
	}

	opModels := []libovsdbops.OperationModel{
		{
			Model: remotePort,
		},
		{
			Model: chassis,
			DoAfter: func() {
				remotePort.Chassis = &chassis.UUID
			},
		},
		{
			Model: remotePort,
			OnModelUpdates: []interface{}{
				&remotePort.Chassis,
			},
		},
	}

	modelSbClient := libovsdbops.NewModelClient(ic.sbClient)
	if _, err := modelSbClient.CreateOrUpdate(opModels...); err != nil {
		return fmt.Errorf("failed to update chassis for remote port, error: %v", portName)
	}

	return nil
}

func (ic *Controller) syncNodeResources(ni nodeInfo) {
	if ni.name == ic.localAzName || (ic.localAzName == types.GlobalAz && ni.isNodeGlobalAz) {
		_ = ic.createLocalAzNodeResources(ni)
	} else {
		_ = ic.createRemoteAzNodeResources(ni)
	}
}

/*
func (ic *Controller) cleanupNodeResources(nodeName string) {
	ni := ic.nodes[nodeName]
	if ni.name != ic.localAzName {
		//TODO
	}
}
*/

func (ic *Controller) createRemoteChassis(node *v1.Node) error {
	if node.Name == ic.localAzName {
		return nil
	}

	chassisID, err := util.ParseNodeChassisIDAnnotation(node)
	if err != nil {
		return fmt.Errorf("failed to parse node chassis-id annotation, error: %v", err)
	}
	remoteNodeIp, err := util.GetNodePrimaryIP(node)
	if err != nil {
		return err
	}

	options := map[string]string{
		"is-remote": "true",
	}

	chassis := sbdb.Chassis{
		Hostname:    node.Name,
		Name:        chassisID,
		ExternalIDs: options,
		OtherConfig: options,
	}
	encap := sbdb.Encap{
		Type:        "geneve",
		ChassisName: chassisID,
		IP:          remoteNodeIp,
		Options:     map[string]string{"csum": "true"},
	}
	modelSbClient := libovsdbops.NewModelClient(ic.sbClient)
	opModels := []libovsdbops.OperationModel{
		{
			Model: &encap,
			OnModelUpdates: []interface{}{
				&encap.Options,
			},
			DoAfter: func() {
				chassis.Encaps = []string{encap.UUID}
			},
		},
		{
			Model:          &chassis,
			ModelPredicate: func(ch *sbdb.Chassis) bool { return ch.Name == chassisID },
			OnModelUpdates: []interface{}{
				&chassis.ExternalIDs,
				&chassis.OtherConfig,
			},
			OnModelMutations: []interface{}{
				&chassis.Encaps,
			},
		},
	}

	if _, err := modelSbClient.CreateOrUpdate(opModels...); err != nil {
		return fmt.Errorf("failed to create encaps for chassis %s, error: %v", node.Name, err)
	}

	return nil
}
