package cluster

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	invalidIPAddress = "0.0.0.1"
	haLeaderLockName = "ovn-kubernetes-master"
	ovnkubeDbEp      = "ovnkube-db"
)

//TODO: Need to make this configurable.
const (
	ovnNorthDbPort int32 = 6641
	ovnSouthDbPort int32 = 6642
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
	kubeClient        kube.Interface
	clusterController *OvnClusterController
	ovnController     *ovn.Controller
	watchFactory      *factory.WatchFactory
	nodeName          string
	isLeader          bool
	leaderElector     *leaderelection.LeaderElector
}

// NewHAMasterController creates a new HA Master controller
func NewHAMasterController(kubeClient kubernetes.Interface, wf *factory.WatchFactory,
	ovnController *ovn.Controller, nodeName string) *OvnHAMasterController {
	return &OvnHAMasterController{
		kubeClient:        &kube.Kube{KClient: kubeClient},
		clusterController: NewClusterController(kubeClient, wf, nodeName),
		ovnController:     ovnController,
		watchFactory:      wf,
		nodeName:          nodeName,
		isLeader:          false,
		leaderElector:     nil,
	}
}

// StartMaster runs the HA master
func (hacluster *OvnHAMasterController) StartMaster() error {
	// Always demote the OVN DBs to backmode mode.
	// After the leader election, the leader will promote the OVN Dbs
	// to become active.
	err := hacluster.DemoteOVNDbs(invalidIPAddress, "6641", "6642")
	if err != nil {
		// If we are not able to communicate to the OVN ovsdb-servers,
		// then it is better to return than continue.
		// cmd/ovnkube.go will panic if this function returns error.
		return err
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

	HAClusterNewLeader := func(nodeName string) {
		logrus.Infof(nodeName + " is the new leader")
		wasLeader := hacluster.isLeader

		if hacluster.nodeName == nodeName {
			logrus.Infof(" I (" + hacluster.nodeName + ") won the election. In active mode")
			err = hacluster.ConfigureAsActive(nodeName)
			if err != nil {
				logrus.Errorf(err.Error())
				// Stop ovn-northd before panicing.
				_, _, _ = util.RunOVNNorthAppCtl("exit")
				panic(err.Error())
			}
			hacluster.isLeader = true
		} else {
			logrus.Infof(" I (" + hacluster.nodeName + ") lost the election. In Standby mode")
			if wasLeader {
				//This node was leader and it lost the election.
				// Whenever the node transitions from leader to follower,
				// we need to handle the transition properly like clearing
				// the cache. It is better to exit for now.
				// kube will restart and this will become a follower.
				// Stop ovn-northd and then exit.
				_, _, _ = util.RunOVNNorthAppCtl("exit")
				logrus.Infof("I (" + hacluster.nodeName + ") am no longer a leader. Exiting")
				os.Exit(1)
			}
			ep, er := hacluster.kubeClient.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
			if er == nil {
				er = hacluster.ConfigureAsStandby(ep)
				if er != nil {
					logrus.Errorf(er.Error())
					// Stop ovn-northd and then exit
					_, _, _ = util.RunOVNNorthAppCtl("exit")
					panic(er.Error())
				}
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
				logrus.Infof("I (" + hacluster.nodeName + ") am no longer a leader. Exiting")
				// Stop ovn-northd and then exit.
				_, _, _ = util.RunOVNNorthAppCtl("exit")
				os.Exit(1)
			},
			OnNewLeader: HAClusterNewLeader,
		},
	}

	//go leaderelection.RunOrDie(context.Background(), lec)
	le, err := leaderelection.NewLeaderElector(lec)
	if err != nil {
		return err
	}

	hacluster.leaderElector = le
	go hacluster.leaderElector.Run(context.Background())

	err = hacluster.WatchOvnDbEndpoints()
	return err
}

// StartNode passes through to the base OvnClusterController node
func (hacluster *OvnHAMasterController) StartNode() error {
	return hacluster.clusterController.StartNode()
}

// ConfigureAsActive configures the node as active.
func (hacluster *OvnHAMasterController) ConfigureAsActive(masterNodeName string) error {
	// Step 1: Update the ovnkube-db endpoints with invalid Ip.
	// Step 2: Promote OVN DB servers to become active
	// Step 3: Make sure that ovn-northd has done one round of
	//         flow computation.
	// Step 4: Update the ovnkube-db endpoints with the new master Ip.

	// Find the endpoint for the service
	ep, err := hacluster.kubeClient.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
	if err != nil {
		ep = nil
	}
	err = hacluster.updateOvnDbEndpoints(ep, true)
	if err != nil {
		logrus.Errorf("%s Endpount create/update failed", ovnkubeDbEp)
		return err
	}

	// Promote the OVN DB servers
	err = hacluster.PromoteOVNDbs(strconv.Itoa(int(ovnNorthDbPort)), strconv.Itoa(int(ovnSouthDbPort)))
	if err != nil {
		logrus.Errorf("Promoting OVN ovsdb-servers to active failed")
		return err
	}

	// Wait for ovn-northd sync up
	err = hacluster.syncOvnNorthd()
	if err != nil {
		logrus.Errorf("Syncing ovn-northd failed")
		return err
	}

	ep, err = hacluster.kubeClient.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
	if err != nil {
		// This should not happen.
		ep = nil
	}
	err = hacluster.updateOvnDbEndpoints(ep, false)
	if err != nil {
		logrus.Errorf("%s Endpount create/update failed", ovnkubeDbEp)
		return err
	}

	// run the cluster controller to init the master
	err = hacluster.StartMaster()
	if err != nil {
		return err
	}

	return hacluster.ovnController.Run()
}

//updateOvnDbEndpoints Updates the ovnkube-db endpoints. Should be called
// only if ovnkube-master is leader. This function will create the ovnkube-db endpoints
// if it doesn't exist.
func (hacluster *OvnHAMasterController) updateOvnDbEndpoints(ep *kapi.Endpoints, configureInvalidIP bool) error {

	var epIP string
	if configureInvalidIP {
		epIP = invalidIPAddress
	} else {
		epIP = config.Kubernetes.PodIP
	}

	epSubsets := []v1.EndpointSubset{
		{
			Addresses: []v1.EndpointAddress{
				{IP: epIP},
			},
			Ports: []v1.EndpointPort{
				{
					Name: "north",
					Port: ovnNorthDbPort,
				},
				{
					Name: "south",
					Port: ovnSouthDbPort,
				},
			},
		},
	}

	var err error
	if ep == nil {
		logrus.Debugf("updateOvnDbEndpoints : Creating the endpoint")
		// Create the endpoint
		ovndbEp := v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: config.Kubernetes.OVNConfigNamespace,
				Name:      ovnkubeDbEp,
			},
			Subsets: epSubsets,
		}

		ovndbEp.ObjectMeta.SetAnnotations(map[string]string{
			"ovnkube-master-leader": hacluster.nodeName})
		_, err = hacluster.kubeClient.CreateEndpoint(config.Kubernetes.OVNConfigNamespace, &ovndbEp)
		if err != nil {
			logrus.Errorf("%s Endpount Create failed", ovnkubeDbEp)
		}
	} else {
		logrus.Debugf("updateOvnDbEndpoints : Updating the endpoint")
		ovndbEp := ep.DeepCopy()
		ovndbEp.Subsets = epSubsets

		epAnnotations := ovndbEp.ObjectMeta.GetAnnotations()
		epAnnotations["ovnkube-master-leader"] = hacluster.nodeName
		ovndbEp.ObjectMeta.SetAnnotations(epAnnotations)
		_, err := hacluster.kubeClient.UpdateEndpoint(config.Kubernetes.OVNConfigNamespace, ovndbEp)
		if err != nil {
			logrus.Errorf("%s Endpount Update failed", ovnkubeDbEp)
		}
	}
	return err
}

// ConfigureAsStandby configures the node as standby
func (hacluster *OvnHAMasterController) ConfigureAsStandby(ep *kapi.Endpoints) error {
	// Get the master ip
	masterIPList, sbDBPort, nbDBPort, err := extractDbRemotesFromEndpoint(ep)
	if err != nil {
		// The db remotes are invalid. Return without doing anything.
		// Once master updates the endpoints properly we will be notified.
		logrus.Infof("ConfigureAsStandby : error in extracting DbRemotes From Endpoint")
		return nil
	}

	logrus.Infof("ConfigureAsStandby: New leader IP is : [%s]", masterIPList[0])

	// Get the active ovsdb-server address.
	var stdout string
	stdout, _, err = util.RunOVNNBAppCtl("ovsdb-server/get-active-ovsdb-server")
	if err != nil {
		logrus.Errorf("Getting  active-ovsdb-server of NB ovsdb-server failed")
		return err
	}

	activeServerOutOfSync := false
	s := strings.Split(stdout, ":")
	if len(s) != 3 {
		activeServerOutOfSync = true
	} else {
		if s[0] != "tcp" || s[1] != masterIPList[0] || s[2] != nbDBPort {
			activeServerOutOfSync = true
		}
	}

	if !activeServerOutOfSync {
		stdout, _, err = util.RunOVNSBAppCtl("ovsdb-server/get-active-ovsdb-server")
		if err != nil {
			logrus.Errorf("Getting  active-ovsdb-server of SB ovsdb-server failed")
			return err
		}

		s := strings.Split(stdout, ":")
		if len(s) != 3 {
			activeServerOutOfSync = true
		} else {
			if s[0] != "tcp" || s[1] != masterIPList[0] || s[2] != sbDBPort {
				activeServerOutOfSync = true
			}
		}
	}

	if activeServerOutOfSync {
		logrus.Debugf("ConfigureAsStandby : active server out of sync..Setting the new active server to : " + masterIPList[0])
		err = hacluster.DemoteOVNDbs(masterIPList[0], nbDBPort, sbDBPort)
		if err != nil {
			logrus.Errorf("Demoting OVN ovsdb-servers to standby failed")
			return err
		}
	}
	return nil
}

func (hacluster *OvnHAMasterController) validateOvnDbEndpoints(ep *kapi.Endpoints) bool {
	if ep.Name != ovnkubeDbEp {
		return false
	}

	epAnnotations := ep.ObjectMeta.GetAnnotations()
	leader, present := epAnnotations["ovnkube-master-leader"]
	if !present || leader != hacluster.leaderElector.GetLeader() {
		return false
	}

	masterIPList, sbDBPort, nbDBPort, err := extractDbRemotesFromEndpoint(ep)
	if err != nil {
		return false
	}

	if masterIPList[0] != config.Kubernetes.PodIP ||
		sbDBPort != strconv.Itoa(int(ovnSouthDbPort)) ||
		nbDBPort != strconv.Itoa(int(ovnNorthDbPort)) {
		return false
	}

	return true
}

// WatchOvnDbEndpoints watches the ovnkube-db end point
func (hacluster *OvnHAMasterController) WatchOvnDbEndpoints() error {
	_, err := hacluster.watchFactory.AddFilteredEndpointsHandler(config.Kubernetes.OVNConfigNamespace,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ep := obj.(*kapi.Endpoints)
				if ep.Name != ovnkubeDbEp {
					return
				}
				if hacluster.leaderElector.IsLeader() {
					if !hacluster.validateOvnDbEndpoints(ep) {
						_ = hacluster.updateOvnDbEndpoints(ep, false)
					}
				} else {
					err := hacluster.ConfigureAsStandby(ep)
					if err != nil {
						logrus.Errorf(err.Error())
						panic(err.Error())
					}
				}
			},
			UpdateFunc: func(old, new interface{}) {
				epNew := new.(*kapi.Endpoints)
				if epNew.Name != ovnkubeDbEp {
					return
				}
				if hacluster.leaderElector.IsLeader() {
					if !validateOVNConfigEndpoint(epNew) {
						_ = hacluster.updateOvnDbEndpoints(epNew, false)
					}
				} else {
					err := hacluster.ConfigureAsStandby(epNew)
					if err != nil {
						logrus.Errorf(err.Error())
						panic(err.Error())
					}
				}
			},
			DeleteFunc: func(obj interface{}) {
				ep := obj.(*kapi.Endpoints)
				if ep.Name == ovnkubeDbEp && hacluster.leaderElector.IsLeader() {
					_ = hacluster.updateOvnDbEndpoints(nil, false)
				}
			},
		}, nil)
	return err
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
	target := "ptcp:" + nbDBPort + ":" + config.Kubernetes.PodIP
	_, _, err = util.RunOVNNBAppCtl("ovsdb-server/add-remote", target)
	if err != nil {
		logrus.Errorf("Adding remote [%s] to NB ovsdb-server failed", target)
		return err
	}

	target = "ptcp:" + sbDBPort + ":" + config.Kubernetes.PodIP
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

func (hacluster *OvnHAMasterController) syncOvnNorthd() error {
	// To sync ovn-northd we do this
	// 1. Get the nb-cfg value from NB_Global table
	//    $nb_nb_cfg=`ovn-nbctl --bare --columns nb_cfg list NB_Global`
	// 2. Get the nb-cfg value from SB_Global table
	//    $sb_nb_cfg=`ovn-sbctl --bare --columns nb_cfg list SB_Global`
	// 3. Increment the value of nb_nb_cfg by 1 and make sure that nb_nb_cfg != sb_nb_cfg
	// 4. Set the nb_nb_cfg in NB_Global table.
	//    $ovn-nbctl set NB_Global. nb_cfg=$nb_nb_cfg
	// 5. Query for nb-cfg in SB_Global table and make sure that it is incremented by 1.
	//    Wait for some time.
	// Return true if sb_nb_cfg gets incremented by 1 within the timeout (30 seconds)
	// Return false otherwise.

	stdout, _, err := util.RunOVNNbctl("--bare", "--columns", "nb_cfg", "list", "NB_Global")
	if err != nil {
		logrus.Errorf("Error in getting NB_Global's nb_cfg column")
		return err
	}

	nbNbCfg, _ := strconv.Atoi(stdout)

	stdout, _, err = util.RunOVNSbctl("--bare", "--columns", "nb_cfg", "list", "SB_Global")
	if err != nil {
		logrus.Errorf("Error in getting SB_Global's nb_cfg column")
		return err
	}

	sbNbCfg, _ := strconv.Atoi(stdout)

	nbNbCfg++
	if nbNbCfg == sbNbCfg {
		nbNbCfg++
	}

	nbCfgValue := "nb_cfg=" + strconv.Itoa(nbNbCfg)
	_, _, err = util.RunOVNNbctl("set", "NB_Global", ".", nbCfgValue)

	if err != nil {
		logrus.Errorf("Error in setting NB_Global's nb_cfg column")
		return err
	}

	if err = wait.PollImmediate(500*time.Millisecond, 30*time.Second, func() (bool, error) {
		stdout, _, err = util.RunOVNSbctl("--bare", "--columns", "nb_cfg", "list", "SB_Global")
		if err != nil {
			logrus.Errorf("Error in getting SB_Global's nb_cfg column")
			return false, err
		}

		sbNbCfg, _ := strconv.Atoi(stdout)
		if nbNbCfg == sbNbCfg {
			return true, nil
		}

		return false, nil
	}); err != nil {
		logrus.Errorf("Error getting the correct nb_cfg value in SB_Global table.")
		return err
	}
	return nil
}
