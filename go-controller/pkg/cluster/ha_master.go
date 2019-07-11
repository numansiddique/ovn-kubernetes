package cluster

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	invalidIPAddres  = "192.0.2.254"
	haLeaderLockName = "ovn-kubernetes-master"
	ovnkubeDbEp      = "ovnkube-db"
)

//TODO: Need to come up with proper default values.
const (
	leaseDuration = 6 * time.Second
	renewDeadline = 3 * time.Second
	retryPeriod   = 2 * time.Second
)

// OvnHAMasterController is the object holder for managing the HA master
// cluster
type OvnHAMasterController struct {
	kubeClient      kubernetes.Interface
	cluster         *OvnClusterController
	ovnController   *ovn.Controller
	nodeName        string
	manageDBServers bool
	isLeader        bool
	currentLeaderIP string
}

// NewHAMasterController creates a new HA Master controller
func NewHAMasterController(kubeClient kubernetes.Interface, cluster *OvnClusterController,
	nodeName string, manageDBServers bool) *OvnHAMasterController {
	ovnController := ovn.NewOvnController(kubeClient, cluster.watchFactory)
	return &OvnHAMasterController{
		kubeClient:      kubeClient,
		cluster:         cluster,
		ovnController:   ovnController,
		nodeName:        nodeName,
		manageDBServers: manageDBServers,
		isLeader:        false,
		currentLeaderIP: "",
	}
}

// StartHAMasterCluster runs the replication controller
func (hacluster *OvnHAMasterController) StartHAMasterCluster() error {
	if hacluster.manageDBServers {
		// Always demote the OVN DBs to backmode mode.
		// After the leader election, the leader will promote the OVN Dbs
		// to become active.
		err := hacluster.DemoteOVNDbs(invalidIPAddres, "6641", "6642")
		if err != nil {
			// If we are not able to communicate to the OVN ovsdb-servers,
			// then it is better to return than continue.
			// cmd/ovnkube.go will panic if this function returns error.
			return err
		}
	}

	// Set up leader election process first
	rl, err := resourcelock.New(
		resourcelock.ConfigMapsResourceLock,
		config.Kubernetes.OVNConfigNamespace,
		haLeaderLockName,
		hacluster.kubeClient.CoreV1(),
		nil,
		resourcelock.ResourceLockConfig{
			Identity:      hacluster.nodeName,
			EventRecorder: nil,
		})

	if err != nil {
		return err
	}

	HAClusterNewLeader := func(identity string) {
		logrus.Infof("The new leader is : " + identity)
		wasLeader := hacluster.isLeader
		if hacluster.nodeName == identity {
			err = hacluster.ConfigureAsActive(identity)
			if err != nil {
				logrus.Errorf(err.Error())
				panic(err.Error())
			}
			hacluster.isLeader = true
		} else {
			if wasLeader {
				//This node was leader and it lost the election.
				// Whenever the node transitions from leader to follower,
				// we need to handle the transition properly like clearing
				// the cache. It is better to exit for now.
				// kube will restart and this will become a follower.
				logrus.Infof("[%s] was the leader and it lost the election. Exiting", hacluster.nodeName)
				os.Exit(1)
			}

			err = hacluster.ConfigureAsStandby(identity)
			if err != nil {
				logrus.Errorf(err.Error())
				panic(err.Error())
			}

		}
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDeadline,
		RetryPeriod:   retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {},
			OnStoppedLeading: func() {
				//This node was leader and it lost the election.
				// Whenever the node transitions from leader to follower,
				// we need to handle the transition properly like clearing
				// the cache. It is better to exit for now.
				// kube will restart and this will become a follower.
				logrus.Infof("[%s] stopped Leading. Exiting", hacluster.nodeName)
				os.Exit(1)
			},
			OnNewLeader: HAClusterNewLeader,
		},
	}

	go leaderelection.RunOrDie(context.Background(), lec)

	return nil
}

// ConfigureAsActive configures the node as active.
func (hacluster *OvnHAMasterController) ConfigureAsActive(masterNodeName string) error {
	if hacluster.manageDBServers {
		// Find the endpoint for the service
		ep, err := hacluster.cluster.Kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
		createEp := true
		if err == nil {
			if ep.Name == ovnkubeDbEp {
				createEp = false
			}
		}

		const (
			nbPort int32 = 6641
			sbPort int32 = 6642
		)

		ovndbEp := v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: config.Kubernetes.OVNConfigNamespace,
				Name:      ovnkubeDbEp,
			},
			Subsets: []v1.EndpointSubset{
				{
					Addresses: []v1.EndpointAddress{
						{IP: config.Kubernetes.OVNKubePodIP},
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
			_, err = hacluster.cluster.Kube.CreateEndpoint(config.Kubernetes.OVNConfigNamespace, &ovndbEp)
			if err != nil {
				logrus.Errorf("%s Endpount Create failed", ovnkubeDbEp)
				return err
			}
		} else {
			_, err = hacluster.cluster.Kube.UpdateEndpoint(config.Kubernetes.OVNConfigNamespace, &ovndbEp)
			if err != nil {
				logrus.Errorf("%s Endpount Update failed", ovnkubeDbEp)
				return err
			}
		}

		// Promote the OVN DB servers.
		err = hacluster.PromoteOVNDbs(strconv.Itoa(int(nbPort)), strconv.Itoa(int(sbPort)))
		if err != nil {
			logrus.Errorf("Promoting OVN ovsdb-servers to active failed")
			return err
		}
	}

	// run the cluster controller to init the master
	err := hacluster.cluster.StartClusterMaster(hacluster.nodeName)
	if err != nil {
		return err
	}

	return hacluster.ovnController.Run()
}

// ConfigureAsStandby configures the node as standby
func (hacluster *OvnHAMasterController) ConfigureAsStandby(masterNodeName string) error {
	if hacluster.manageDBServers {
		if err := wait.PollImmediate(500*time.Millisecond, 5*time.Second, func() (bool, error) {
			// Get the ovndb endpoint.
			ep, err := hacluster.cluster.Kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
			if err != nil {
				logrus.Infof("[%s] Endpount Not found.", ovnkubeDbEp)
				return false, nil
			}

			// Get the master ip
			masterIPList, sbDBPort, nbDBPort, err := extractDbRemotesFromEndpoint(ep)
			if err != nil {
				return false, nil
			}

			if hacluster.currentLeaderIP == masterIPList[0] {
				// Looks like new master has not yet updated the ovnkube-db endpoint.
				// Wait for 500 ms.
				return false, nil
			}

			hacluster.currentLeaderIP = masterIPList[0]
			logrus.Infof("New leader IP is : [%s]", hacluster.currentLeaderIP)

			err = hacluster.DemoteOVNDbs(hacluster.currentLeaderIP, nbDBPort, sbDBPort)
			if err != nil {
				logrus.Errorf("Demoting OVN ovsdb-servers to standby failed")
				return false, err
			}

			return true, nil
		}); err != nil {
			logrus.Errorf("Error getting the correct endpoint [%s]", ovnkubeDbEp)
			return err
		}
	}

	hacluster.cluster.StopClusterMaster()

	return nil
}

//PromoteOVNDbs promotes the OVN Db servers and resumes the ovn-northd.
func (hacluster *OvnHAMasterController) PromoteOVNDbs(nbDBPort, sbDBPort string) error {
	_, _, err := util.RunOVNCtl("promote_ovnnb")
	if err != nil {
		logrus.Errorf("promoting ovnnb failed")
		return err
	}
	_, _, err = util.RunOVNCtl("promote_ovnsb")
	if err != nil {
		logrus.Errorf("promoting ovnsb failed")
		return err
	}

	// Configure OVN dbs to listen on the ovnkube-pod-ip
	target := "ptcp:" + nbDBPort + ":" + config.Kubernetes.OVNKubePodIP
	_, _, err = util.RunOVNNBAppCtl("ovsdb-server/add-remote", target)
	if err != nil {
		logrus.Errorf("Adding remote [%s] to NB ovsdb-server failed", target)
		return err
	}

	target = "ptcp:" + sbDBPort + ":" + config.Kubernetes.OVNKubePodIP
	_, _, err = util.RunOVNSBAppCtl("ovsdb-server/add-remote", target)
	if err != nil {
		logrus.Errorf("Adding remote [%s] to SB ovsdb-server failed", target)
		return err
	}

	// Ignore the error from this command for now as the patch in OVN
	// to add the pause/resume support is not yet merged.
	//TODO: Handle the err properly when ovn-northd supports pause/resume
	_, _, _ = util.RunOVNNorthAppCtl("resume")
	return nil
}

//DemoteOVNDbs demotes the OVN Db servers and configure them to connect
//to the new master and pauses the ovn-northd.
func (hacluster *OvnHAMasterController) DemoteOVNDbs(masterIP, nbDBPort, sbDBPort string) error {
	_, _, err := util.RunOVNCtl("demote_ovnnb", "--db-nb-sync-from-addr="+masterIP,
		"--db-nb-sync-from-port="+nbDBPort)
	if err != nil {
		logrus.Errorf("Demoting NB ovsdb-server failed")
		return err
	}

	_, _, err = util.RunOVNCtl("demote_ovnsb", "--db-sb-sync-from-addr="+masterIP,
		"--db-sb-sync-from-port="+sbDBPort)

	if err != nil {
		logrus.Errorf("Demoting SB ovsdb-server failed")
		return err
	}

	// Ignore the error from this command for now as the patch in OVN
	// to add the pause/resume support is not yet merged.
	//TODO: Handle the err properly when ovn-northd supports pause/resume
	_, _, _ = util.RunOVNNorthAppCtl("pause")
	return nil
}
