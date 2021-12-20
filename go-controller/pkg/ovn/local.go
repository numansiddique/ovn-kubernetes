package ovn

import (
	"fmt"
	"net"
	"sync"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// Local Controller structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints) on each
// node configured as a local AZ node.
type LocalController struct {
	nodeName     string
	client       clientset.Interface
	kube         kube.Interface
	watchFactory *factory.WatchFactory
	stopChan     <-chan struct{}

	oc *Controller
}

// NewLocalOvnController creates a new OVN controller for creating logical network
// infrastructure and policy
func NewLocalOvnController(ovnClient *util.OVNClientset, wf *factory.WatchFactory, stopChan <-chan struct{}, addressSetFactory addressset.AddressSetFactory,
	libovsdbOvnNBClient libovsdbclient.Client, libovsdbOvnSBClient libovsdbclient.Client,
	recorder record.EventRecorder, nodeName string) *LocalController {
	if addressSetFactory == nil {
		addressSetFactory = addressset.NewOvnAddressSetFactory(libovsdbOvnNBClient)
	}
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
		watchFactory: wf,
		stopChan:     stopChan,
		oc:           oc,
	}
}

func (lc *LocalController) Start(wg *sync.WaitGroup) error {
	wg.Add(1)
	go func() {
		_ = lc.Run(wg)
		klog.Infof("Stopped local controller")
		wg.Done()
	}()

	return nil
}

func (lc *LocalController) Run(wg *sync.WaitGroup) error {
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

	_ = lc.oc.SetupMaster(lc.nodeName, make([]string, 0))
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
