package clustermanager

import (
	"context"
	"os"
	"reflect"
	"sync"
	"time"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	clientset "k8s.io/client-go/kubernetes"
)

// ClusterManager structure is the object which manages the cluster nodes.
type ClusterManager struct {
	client               clientset.Interface
	defaultNetClusterMgr *defaultNetworkClusterMgr
	wf                   *factory.WatchFactory
	stopChan             chan struct{}
	// event recorder used to post events to k8s
	recorder record.EventRecorder
}

// NewOvnController creates a new OVN controller for creating logical network
// infrastructure and policy
func NewClusterManager(ovnClient *util.OVNClientset, wf *factory.WatchFactory, stopChan chan struct{},
	wg *sync.WaitGroup, recorder record.EventRecorder) *ClusterManager {
	cm := &ClusterManager{
		client:               ovnClient.KubeClient,
		defaultNetClusterMgr: newDefaultNetworkClusterManager(ovnClient, wf, stopChan, wg),
		wf:                   wf,
		recorder:             recorder,
		stopChan:             stopChan,
	}

	return cm
}

type ovnkubeClusterManagerLeaderMetrics struct{}

func (ovnkubeClusterManagerLeaderMetrics) On(string) {
	metrics.MetricClusterManagerLeader.Set(1)
}

func (ovnkubeClusterManagerLeaderMetrics) Off(string) {
	metrics.MetricClusterManagerLeader.Set(0)
}

type ovnkubeClusterManagerLeaderMetricsProvider struct{}

func (_ ovnkubeClusterManagerLeaderMetricsProvider) NewLeaderMetric() leaderelection.SwitchMetric {
	return ovnkubeClusterManagerLeaderMetrics{}
}

// Start waits until this process is the leader before starting master functions
func (cm *ClusterManager) Start(nodeName string, wg *sync.WaitGroup, ctx context.Context) error {
	klog.Infof("Cluster manager Started.")
	// Set up leader election process first.
	// User lease resource lock as configmap and endpoint lock support is removed from leaderelection library.
	rl, err := resourcelock.New(
		resourcelock.LeasesResourceLock,
		config.Kubernetes.OVNConfigNamespace,
		"ovn-kubernetes-cluster-manager",
		cm.client.CoreV1(),
		cm.client.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      nodeName,
			EventRecorder: cm.recorder,
		},
	)
	if err != nil {
		return err
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   time.Duration(config.ClusterMgrHA.ElectionLeaseDuration) * time.Second,
		RenewDeadline:   time.Duration(config.ClusterMgrHA.ElectionRenewDeadline) * time.Second,
		RetryPeriod:     time.Duration(config.ClusterMgrHA.ElectionRetryPeriod) * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.Infof("Won leader election; in active mode")
				// run the cluster controller to init the cluster manager
				start := time.Now()
				defer func() {
					end := time.Since(start)
					metrics.MetriClusterManagerReadyDuration.Set(end.Seconds())
				}()

				if err := cm.StartClusterManager(); err != nil {
					panic(err.Error())
				}
				// run the cluster controller to init the master
				// run only on the active master node.
				if err := cm.Run(); err != nil {
					panic(err.Error())
				}
			},
			OnStoppedLeading: func() {
				//This node was leader and it lost the election.
				// Whenever the node transitions from leader to follower,
				// we need to handle the transition properly like clearing
				// the cache. It is better to exit for now.
				// kube will restart and this will become a follower.
				klog.Infof("No longer leader; exiting")
				os.Exit(0)
			},
			OnNewLeader: func(newLeaderName string) {
				if newLeaderName != nodeName {
					klog.Infof("Lost the election to %s; in standby mode", newLeaderName)
				}
			},
		},
	}

	leaderelection.SetProvider(ovnkubeClusterManagerLeaderMetricsProvider{})
	leaderElector, err := leaderelection.NewLeaderElector(lec)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		leaderElector.Run(ctx)
		klog.Infof("Stopped leader election")
		wg.Done()
	}()

	return nil
}

func (cm *ClusterManager) Stop() {
	metrics.UnRegisterClusterManagerFunctional()
	close(cm.stopChan)
	cm.defaultNetClusterMgr.Stop()
}

// StartClusterManager runs a subnet IPAM that watches arrival/departure
// of nodes in the cluster
// On an addition to the cluster (node create), a new subnet is created for it.
// ovnkube-master will create the node logical switch and other resources in the
// OVN Northbound database.
// Upon deletion of a node, the node subnet is released.
//
// TODO: Verify that the cluster was not already called with a different global subnet
//
//	If true, then either quit or perform a complete reconfiguration of the cluster (recreate switches/routers with new subnet values)
func (cm *ClusterManager) StartClusterManager() error {
	klog.Infof("Starting cluster manager")
	metrics.RegisterClusterManagerFunctional()

	return cm.defaultNetClusterMgr.Start()
}

// Run starts the actual watching.
func (cm *ClusterManager) Run() error {
	// Start and sync the watch factory to begin listening for events
	if err := cm.wf.Start(); err != nil {
		return err
	}

	return cm.defaultNetClusterMgr.Run()
}

// hasResourceAnUpdateFunc returns true if the given resource type has a dedicated update function.
// It returns false if, upon an update event on this resource type, we instead need to first delete the old
// object and then add the new one.
func hasResourceAnUpdateFunc(objType reflect.Type) bool {
	switch objType {
	case factory.NodeType:
		return true
	}
	return false
}
