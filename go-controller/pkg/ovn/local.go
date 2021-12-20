package ovn

import (
	"fmt"
	"net"
	"sync"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	egressipv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressip/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	svccontroller "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/controller/services"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/libovsdbops"
	lsm "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/logical_switch_manager"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/subnetallocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// Local Controller structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints) on each
// node configured as a local AZ node.
type LocalController struct {
	nodeName              string
	client                clientset.Interface
	kube                  kube.Interface
	watchFactory          *factory.WatchFactory
	egressFirewallHandler *factory.Handler
	stopChan              <-chan struct{}

	// FIXME DUAL-STACK -  Make IP Allocators more dual-stack friendly
	masterSubnetAllocator *subnetallocator.SubnetAllocator

	oc *Controller

	SCTPSupport bool

	// For TCP, UDP, and SCTP type traffic, cache OVN load-balancers used for the
	// cluster's east-west traffic.
	loadbalancerClusterCache map[kapi.Protocol]string

	// A cache of all logical switches seen by the watcher and their subnets
	lsManager *lsm.LogicalSwitchManager

	// A cache of all logical ports known to the controller
	logicalPortCache *portCache

	// Info about known namespaces. You must use oc.getNamespaceLocked() or
	// oc.waitForNamespaceLocked() to read this map, and oc.createNamespaceLocked()
	// or oc.deleteNamespaceLocked() to modify it. namespacesMutex is only held
	// from inside those functions.
	namespaces      map[string]*namespaceInfo
	namespacesMutex sync.Mutex

	externalGWCache map[ktypes.NamespacedName]*externalRouteInfo
	exGWCacheMutex  sync.RWMutex

	// egressFirewalls is a map of namespaces and the egressFirewall attached to it
	egressFirewalls sync.Map

	// An address set factory that creates address sets
	addressSetFactory addressset.AddressSetFactory

	// For each logical port, the number of network policies that want
	// to add a ingress deny rule.
	lspIngressDenyCache map[string]int

	// For each logical port, the number of network policies that want
	// to add a egress deny rule.
	lspEgressDenyCache map[string]int

	// A mutex for lspIngressDenyCache and lspEgressDenyCache
	lspMutex *sync.Mutex

	// Supports multicast?
	multicastSupport bool

	// Cluster wide Load_Balancer_Group UUID.
	loadBalancerGroupUUID string

	// Controller used for programming OVN for egress IP
	eIPC egressIPController

	// Controller used to handle services
	svcController *svccontroller.Controller
	// svcFactory used to handle service related events
	svcFactory informers.SharedInformerFactory

	egressFirewallDNS *EgressDNS

	// Is ACL logging enabled while configuring meters?
	aclLoggingEnabled bool

	joinSwIPManager *lsm.JoinSwitchIPManager

	// event recorder used to post events to k8s
	recorder record.EventRecorder

	// libovsdb northbound client interface
	nbClient libovsdbclient.Client

	// libovsdb southbound client interface
	sbClient libovsdbclient.Client

	modelClient libovsdbops.ModelClient

	// v4HostSubnetsUsed keeps track of number of v4 subnets currently assigned to nodes
	v4HostSubnetsUsed float64

	// v6HostSubnetsUsed keeps track of number of v6 subnets currently assigned to nodes
	v6HostSubnetsUsed float64

	// Map of pods that need to be retried, and the timestamp of when they last failed
	retryPods     map[types.UID]*retryEntry
	retryPodsLock sync.Mutex

	// channel to indicate we need to retry pods immediately
	retryPodsChan chan struct{}

	metricsRecorder *metrics.ControlPlaneRecorder
}

// NewLocalOvnController creates a new OVN controller for creating logical network
// infrastructure and policy
func NewLocalOvnController(ovnClient *util.OVNClientset, wf *factory.WatchFactory, stopChan <-chan struct{}, addressSetFactory addressset.AddressSetFactory,
	libovsdbOvnNBClient libovsdbclient.Client, libovsdbOvnSBClient libovsdbclient.Client,
	recorder record.EventRecorder, nodeName string) *LocalController {
	if addressSetFactory == nil {
		addressSetFactory = addressset.NewOvnAddressSetFactory(libovsdbOvnNBClient)
	}
	modelClient := libovsdbops.NewModelClient(libovsdbOvnNBClient)
	svcController, svcFactory := newServiceController(ovnClient.KubeClient, libovsdbOvnNBClient)
	oc := NewOvnController(ovnClient, wf, stopChan, addressSetFactory, libovsdbOvnNBClient, libovsdbOvnSBClient, recorder, true, nodeName)
	return &LocalController{
		nodeName: nodeName,
		client:   ovnClient.KubeClient,
		kube: &kube.Kube{
			KClient:              ovnClient.KubeClient,
			EIPClient:            ovnClient.EgressIPClient,
			EgressFirewallClient: ovnClient.EgressFirewallClient,
			CloudNetworkClient:   ovnClient.CloudNetworkClient,
		},
		watchFactory:        wf,
		stopChan:            stopChan,
		oc:                  oc,
		lsManager:           lsm.NewLogicalSwitchManager(),
		logicalPortCache:    newPortCache(stopChan),
		namespaces:          make(map[string]*namespaceInfo),
		namespacesMutex:     sync.Mutex{},
		externalGWCache:     make(map[ktypes.NamespacedName]*externalRouteInfo),
		exGWCacheMutex:      sync.RWMutex{},
		addressSetFactory:   addressSetFactory,
		lspIngressDenyCache: make(map[string]int),
		lspEgressDenyCache:  make(map[string]int),
		lspMutex:            &sync.Mutex{},
		eIPC: egressIPController{
			egressIPAssignmentMutex: &sync.Mutex{},
			podAssignmentMutex:      &sync.Mutex{},
			podAssignment:           make(map[string][]egressipv1.EgressIPStatusItem),
			allocator:               allocator{&sync.Mutex{}, make(map[string]*egressNode)},
			nbClient:                libovsdbOvnNBClient,
			modelClient:             modelClient,
			watchFactory:            wf,
		},
		loadbalancerClusterCache: make(map[kapi.Protocol]string),
		multicastSupport:         config.EnableMulticast,
		loadBalancerGroupUUID:    "",
		aclLoggingEnabled:        true,
		joinSwIPManager:          nil,
		retryPods:                make(map[types.UID]*retryEntry),
		retryPodsChan:            make(chan struct{}, 1),
		recorder:                 recorder,
		nbClient:                 libovsdbOvnNBClient,
		sbClient:                 libovsdbOvnSBClient,
		svcController:            svcController,
		svcFactory:               svcFactory,
		modelClient:              modelClient,
		metricsRecorder:          metrics.NewControlPlaneRecorder(libovsdbOvnSBClient),
	}
}

func (lc *LocalController) Start(wg *sync.WaitGroup) error {
	klog.Infof("Nums : local OvnController start() entered for : %q", lc.nodeName)
	wg.Add(1)
	go func() {
		lc.Run(wg)
		klog.Infof("Stopped local controller")
		wg.Done()
	}()

	klog.Infof("Nums : local OvnController start() done for : %q", lc.nodeName)
	return nil
}

func (lc *LocalController) Run(wg *sync.WaitGroup) error {
	klog.Infof("Nums : local OvnController Run() entered for : %q", lc.nodeName)
	var err error
	var node *kapi.Node
	var subnets []*net.IPNet

	// First wait for the node logical switch to be created by the Master, timeout is 300s.
	err = wait.PollImmediate(500*time.Millisecond, 300*time.Second, func() (bool, error) {
		if node, err = lc.kube.GetNode(lc.nodeName); err != nil {
			klog.Infof("Waiting to retrieve node %s: %v", lc.nodeName, err)
			return false, nil
		}
		subnets, err = util.ParseNodeHostSubnetAnnotation(node)
		if err != nil {
			klog.Infof("Waiting for node %s to start, no annotation found on node for subnet: %v", lc.nodeName, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("timed out waiting for node's: %q logical switch: %v", lc.nodeName, err)
	}
	klog.Infof("Node %s ready for ovn initialization with subnet %s", lc.nodeName, util.JoinIPNets(subnets, ","))

	lc.oc.SetupMaster(lc.nodeName, make([]string, 0))
	err = lc.oc.ensureNodeLogicalNetwork(node, subnets)
	if err != nil {
		return err
	}
	// Start and sync the watch factory to begin listening for events
	if err := lc.watchFactory.Start(); err != nil {
		return err
	}

	klog.Infof("Starting all the Watchers...")
	//start := time.Now()

	lc.WatchNamespaces()

	lc.WatchNodes()

	lc.oc.WatchPods()
	return nil
}

func (lc *LocalController) WatchNamespaces() {
}

func (lc *LocalController) WatchNodes() {
	lc.watchFactory.AddNodeHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node := obj.(*kapi.Node)
			if node.Name != lc.nodeName {
				// We are only interested in the local node
				return
			}

			klog.Infof("Node %s added", node.Name)

			subnets, err := util.ParseNodeHostSubnetAnnotation(node)
			if err != nil {
				klog.Infof("Waiting for node %s to start, no annotation found on node for subnet: %v", lc.nodeName, err)
				return
			}

			err = lc.oc.ensureNodeLogicalNetwork(node, subnets)
			if err != nil {
				return
			}

			if err = lc.oc.syncNodeClusterRouterPort(node, subnets); err != nil {
				if !util.IsAnnotationNotSetError(err) {
					klog.Warningf(err.Error())
				}
			}

			err = lc.oc.syncNodeManagementPort(node, subnets)
			if err != nil {
				if !util.IsAnnotationNotSetError(err) {
					klog.Warningf("Error creating management port for node %s: %v", node.Name, err)
				}
				return
			}

			if err := lc.oc.syncNodeGateway(node, subnets); err != nil {
				if !util.IsAnnotationNotSetError(err) {
					klog.Warningf(err.Error())
				}
				return
			}

			// ensure pods that already exist on this node have their logical ports created
			//options := metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", node.Name).String()}
			//pods, err := oc.client.CoreV1().Pods(metav1.NamespaceAll).List(context.TODO(), options)
			//if err != nil {
			//	klog.Errorf("Unable to list existing pods on node: %s, existing pods on this node may not function")
			//} else {
			//	oc.addRetryPods(pods.Items)
			//	oc.requestRetryPods()
			//}
		},
		UpdateFunc: func(old, new interface{}) {
		},
		DeleteFunc: func(obj interface{}) {
		},
	}, nil)
}
