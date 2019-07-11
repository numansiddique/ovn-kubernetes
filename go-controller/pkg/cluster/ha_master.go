package cluster

import (
	"context"
	"reflect"
	"strconv"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
)

const (
	InvalidIPAddres = "192.0.2.254"
)

// StartHAMasterCluster runs the replication controller
func (cluster *OvnClusterController) StartHAMasterCluster(masterNodeName string) error {
	logrus.Infof("Hi Numan : StartHAMasterCluster entered")

	// Create haWatchFactory to watch for the ovnkube-db endpoints.
	stopChan := make(chan struct{})
	factory, err := factory.NewWatchFactory(cluster.kubeClient, stopChan)
	if err != nil {
		panic(err.Error())
	}
	cluster.haWatchFactory = factory

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: cluster.kubeClient.CoreV1().Events("")})
	eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "ovn-kubernetes"})

	rl, err := resourcelock.New(
		resourcelock.ConfigMapsResourceLock,
		"ovn-kubernetes",
		"ovn-kubernetes-master",
		cluster.kubeClient.CoreV1(),
		resourcelock.ResourceLockConfig{
			Identity:      masterNodeName,
			EventRecorder: eventRecorder,
		})
	if err != nil {
		return err
	}

	HAClusterNewLeader := func(identity string) {
		logrus.Infof("NUMA: dude ReplNewLeader : the new leader is : " + identity)
		if masterNodeName == identity {
			cluster.ConfigureAsActive(identity)
		} else {
			cluster.ConfigureAsStandby(identity)
		}
	}

	go leaderelection.RunOrDie(context.Background(),
		leaderelection.LeaderElectionConfig{
			Lock:          rl,
			LeaseDuration: 6 * time.Second,
			RenewDeadline: 3 * time.Second,
			RetryPeriod:   2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					logrus.Infof("NUMA: OnStartedLeading called")
				},
				OnStoppedLeading: func() {
					logrus.Infof("NUMA: OnStoppedLeading called")
				},
				OnNewLeader: HAClusterNewLeader,
			},
		})

	//cluster.WatchOvnDbEndpoints()
	return nil
}

// WatchOvnDbEndpoints watches the ovnkube-db end point
func (cluster *OvnClusterController) WatchOvnDbEndpoints() error {
	_, err := cluster.haWatchFactory.AddFilteredEndpointsHandler(config.Kubernetes.OVNConfigNamespace,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ep := obj.(*kapi.Endpoints)
				if ep.Name == "ovnkube-db" {
					updateOVNConfig(ep)
					return
				}
			},
			UpdateFunc: func(old, new interface{}) {
				epNew := new.(*kapi.Endpoints)
				epOld := old.(*kapi.Endpoints)
				if !reflect.DeepEqual(epNew.Subsets, epOld.Subsets) && epNew.Name == "ovnkube-db" {
					updateOVNConfig(epNew)
				}

			},
		}, nil)
	return err
}

// ConfigureAsActive configures the node as active.
func (cluster *OvnClusterController) ConfigureAsActive(masterNodeName string) error {
	logrus.Infof("Phew : I " + masterNodeName + " is the Master .. hahaha")

	// Find the endpoint for the service
	ovnkubeDbEp := "ovnkube-db"
	ep, err := cluster.Kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
	createEp := true
	if err != nil {
		logrus.Infof("%s Endpoint not found. TODO: Need to create it", ovnkubeDbEp)
	} else {
		logrus.Infof("%s Endpount found.", ovnkubeDbEp)
		if ep.Name == ovnkubeDbEp {
			createEp = false
		}
	}

	const (
		nbPort int32 = 6641
		sbPort int32 = 6642
	)

	var masterAddress string
	if masterNodeName == "ovn_c1" {
		masterAddress = "192.168.1.41"
	} else if masterNodeName == "ovn_c2" {
		masterAddress = "192.168.1.42"
	} else if masterNodeName == "ovn_c3" {
		masterAddress = "192.168.1.43"
	} else {
		masterAddress = "192.168.1.44"
	}

	ovndbEp := v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ovn-kubernetes", Name: "ovnkube-db"},
		Subsets: []v1.EndpointSubset{
			{
				Addresses: []v1.EndpointAddress{
					{IP: masterAddress},
				},
				Ports: []v1.EndpointPort{
					{
						Name: "north",
						Port: nbPort,
					},
					{
						Name: "south",
						Port: sbPort,
					},
				},
			},
		},
	}

	if createEp {
		logrus.Infof("%s Endpount Not found. Need to create it", ovnkubeDbEp)
		ep, err := cluster.Kube.CreateEndpoint(config.Kubernetes.OVNConfigNamespace, &ovndbEp)
		if err != nil {
			logrus.Infof("%s Endpount Create failed dude", ovnkubeDbEp)
		} else {
			for _, s := range ep.Subsets {
				for _, ip := range s.Addresses {
					logrus.Infof("ep IP = [%s]", ip.IP)
					for _, port := range s.Ports {
						logrus.Infof("ep port = [%d]", port.Port)
					}
				}
			}
		}
	} else {
		ep, err := cluster.Kube.UpdateEndpoint(config.Kubernetes.OVNConfigNamespace, &ovndbEp)
		if err != nil {
			logrus.Infof("%s Endpount Update failed dude", ovnkubeDbEp)
		} else {
			logrus.Infof("Printing the updated ep")
			for _, s := range ep.Subsets {
				for _, ip := range s.Addresses {
					logrus.Infof("ep IP = [%s]", ip.IP)
					for _, port := range s.Ports {
						logrus.Infof("ep port = [%d]", port.Port)
					}
				}
			}
		}
	}

	// Promote the OVN DB servers and resume ovn-northd
	cluster.PromoteOVNDbs()

	// run the cluster controller to init the master
	cluster.StopClusterMaster()

	stopChan := make(chan struct{})
	factory, err := factory.NewWatchFactory(cluster.kubeClient, stopChan)
	if err != nil {
		panic(err.Error())
	}
	cluster.watchFactory = factory

	err = cluster.StartClusterMaster(masterNodeName)
	if err != nil {
		logrus.Errorf(err.Error())
		panic(err.Error())
	}

	cluster.ovnController = ovn.NewOvnController(cluster.kubeClient, cluster.watchFactory)
	if err := cluster.ovnController.Run(); err != nil {
		logrus.Errorf(err.Error())
		panic(err.Error())
	}

	return nil
}

// ConfigureAsStandby configures the node as standby
func (cluster *OvnClusterController) ConfigureAsStandby(masterNodeName string) error {
	logrus.Infof("Ohh : I am NOT the MASTER ... the new damn master is : " + masterNodeName)
	// Get the ovndb endpoint.
	ep, err := cluster.Kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, "ovnkube-db")
	if err != nil {
		logrus.Infof("ovnkube-db Endpount Not found. Need to handle this")
		return err
	}

	// Get the master ip
	masterIP := ep.Subsets[0].Addresses[0].IP
	logrus.Infof("The master ip is : [%s]", masterIP)
	var nbDBPort string
	var sbDBPort string
	for _, ovnDB := range ep.Subsets[0].Ports {
		if ovnDB.Name == "north" {
			nbDBPort = strconv.Itoa(int(ovnDB.Port))
		}
		if ovnDB.Name == "south" {
			sbDBPort = strconv.Itoa(int(ovnDB.Port))
		}
	}

	cluster.DemoteOVNDbs(masterIP, nbDBPort, sbDBPort)
	cluster.StopClusterMaster()

	return nil
}

//PromoteOVNDbs promotes the OVN Db servers and resumes the ovn-northd.
// It returns only after confirming that the OVN Db servers are promoted
// to become active.
func (cluster *OvnClusterController) PromoteOVNDbs() error {
	stdout, stderr, err := util.RunOVNCtl("promote_ovnnb")
	logrus.Infof("promote_ovnnb : stdout - [%s] : stderr = [%s]", stdout, stderr)
	if err != nil {
		logrus.Infof("promoting ovnnb failed")
	}
	stdout, stderr, err = util.RunOVNCtl("promote_ovnsb")
	if err != nil {
		logrus.Infof("promoting ovnsb failed")
	}
	return nil
}

//DemoteOVNDbs demotes the OVN Db servers and configure them to connect
//to the new master and pauses the ovn-northd.
func (cluster *OvnClusterController) DemoteOVNDbs(masterIP, nbDBPort, sbDBPort string) error {
	// Get the master ip

	util.RunOVNCtl("demote_ovnnb", "--db-nb-sync-from-addr="+masterIP,
		"--db-nb-sync-from-port="+nbDBPort)
	util.RunOVNCtl("demote_ovnsb", "--db-sb-sync-from-addr="+masterIP,
		"--db-sb-sync-from-port="+sbDBPort)
	return nil
}
