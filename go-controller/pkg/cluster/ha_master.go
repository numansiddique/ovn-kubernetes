package cluster

import (
	"context"
	"strconv"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	}
}

// StartHAMasterCluster runs the replication controller
func (hacluster *OvnHAMasterController) StartHAMasterCluster() error {
	if hacluster.manageDBServers {
		// Always demote the OVN DBs to backmode mode.
		// After the leader election, the leader will promote the OVN Dbs
		// to become active.
		_ = hacluster.DemoteOVNDbs(invalidIPAddres, "6641", "6642")
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
		if hacluster.nodeName == identity {
			_ = hacluster.ConfigureAsActive(identity)
		} else {
			_ = hacluster.ConfigureAsStandby(identity)
		}
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDeadline,
		RetryPeriod:   retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {},
			OnStoppedLeading: func() {},
			OnNewLeader:      HAClusterNewLeader,
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
			_, err := hacluster.cluster.Kube.CreateEndpoint(config.Kubernetes.OVNConfigNamespace, &ovndbEp)
			if err != nil {
				//TODO: Handle this situation properly.
				logrus.Infof("%s Endpount Create failed", ovnkubeDbEp)
			}
		} else {
			_, err := hacluster.cluster.Kube.UpdateEndpoint(config.Kubernetes.OVNConfigNamespace, &ovndbEp)
			if err != nil {
				//TODO: Handle this situation properly.
				logrus.Infof("%s Endpount Update failed", ovnkubeDbEp)
			}
		}

		// Promote the OVN DB servers.
		_ = hacluster.PromoteOVNDbs(strconv.Itoa(int(nbPort)), strconv.Itoa(int(sbPort)))
	}

	// run the cluster controller to init the master
	err := hacluster.cluster.StartClusterMaster(hacluster.nodeName)
	if err != nil {
		logrus.Errorf(err.Error())
		panic(err.Error())
	}

	if err := hacluster.ovnController.Run(); err != nil {
		logrus.Errorf(err.Error())
		panic(err.Error())
	}

	return nil
}

// ConfigureAsStandby configures the node as standby
func (hacluster *OvnHAMasterController) ConfigureAsStandby(masterNodeName string) error {
	if hacluster.manageDBServers {
		// Get the ovndb endpoint.
		ep, err := hacluster.cluster.Kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
		if err != nil {
			logrus.Infof("[%s] Endpount Not found. Need to handle this", ovnkubeDbEp)
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

		_ = hacluster.DemoteOVNDbs(masterIP, nbDBPort, sbDBPort)
	}

	hacluster.cluster.StopClusterMaster()

	return nil
}

//PromoteOVNDbs promotes the OVN Db servers and resumes the ovn-northd.
func (hacluster *OvnHAMasterController) PromoteOVNDbs(nbDBPort, sbDBPort string) error {
	_, _, err := util.RunOVNCtl("promote_ovnnb")
	if err != nil {
		//TODO: Need to handle this.
		logrus.Errorf("promoting ovnnb failed")
	}
	_, _, err = util.RunOVNCtl("promote_ovnsb")
	if err != nil {
		//TODO: Need to handle this.
		logrus.Errorf("promoting ovnsb failed")
	}

	// Configure OVN dbs to listen on the ovnkube-pod-ip
	target := "ptcp:" + nbDBPort + ":" + config.Kubernetes.OVNKubePodIP
	_, _, err = util.RunOVNNBAppCtl("ovsdb-server/add-remote", target)
	if err != nil {
		//TODO: Need to handle this.
		logrus.Errorf("ovn-nbctl set-connection [%s] failed", target)
	}

	target = "ptcp:" + sbDBPort + ":" + config.Kubernetes.OVNKubePodIP
	_, _, err = util.RunOVNSBAppCtl("ovsdb-server/add-remote", target)
	if err != nil {
		//TODO: Need to handle this.
		logrus.Errorf("ovn-sbctl set-connection [%s] failed", target)
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
		logrus.Errorf("Demote ovnnb failed")
		return err
	}

	_, _, err = util.RunOVNCtl("demote_ovnsb", "--db-sb-sync-from-addr="+masterIP,
		"--db-sb-sync-from-port="+sbDBPort)

	if err != nil {
		logrus.Errorf("Demote ovnsb failed")
		return err
	}

	// Ignore the error from this command for now as the patch in OVN
	// to add the pause/resume support is not yet merged.
	//TODO: Handle the err properly when ovn-northd supports pause/resume
	_, _, _ = util.RunOVNNorthAppCtl("pause")
	return nil
}
