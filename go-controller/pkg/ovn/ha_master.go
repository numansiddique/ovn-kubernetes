package ovn

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
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
	haMasterLeader   = "ovnkube-master-leader"
)

//TODO: Need to make this configurable.
const (
	ovnNorthDbPort int32 = 6641
	ovnSouthDbPort int32 = 6642
)

// These default values can be further refined.
const (
	leaseDuration = 60 * time.Second
	renewDeadline = 35 * time.Second
	retryPeriod   = 10 * time.Second
)

// HAMasterController is the object holder for managing the HA master
// cluster
type HAMasterController struct {
	kubeClient      kubernetes.Interface
	ovnController   *Controller
	nodeName        string
	manageDBServers bool
	isLeader        bool
	leaderElector   *leaderelection.LeaderElector
}

// NewHAMasterController creates a new HA Master controller
func NewHAMasterController(kubeClient kubernetes.Interface, wf *factory.WatchFactory,
	nodeName string, manageDBServers bool) *HAMasterController {
	ovnController := NewOvnController(kubeClient, wf)
	return &HAMasterController{
		kubeClient:      kubeClient,
		ovnController:   ovnController,
		nodeName:        nodeName,
		manageDBServers: manageDBServers,
		isLeader:        false,
		leaderElector:   nil,
	}
}

// StartHAMasterController runs the replication controller
func (hacontroller *HAMasterController) StartHAMasterController() error {
	if hacontroller.manageDBServers {
		// Always demote the OVN DBs to backup mode.
		// After the leader election, the leader will promote the OVN Dbs
		// to become active.
		err := hacontroller.DemoteOVNDbs(invalidIPAddress, ovnNorthDbPort, ovnSouthDbPort)
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
		hacontroller.kubeClient.CoreV1(),
		nil,
		resourcelock.ResourceLockConfig{
			Identity:      hacontroller.nodeName,
			EventRecorder: nil,
		})

	if err != nil {
		return err
	}

	hacontrollerOnStoppedLeading := func() {
		//This node was leader and it lost the election.
		// Whenever the node transitions from leader to follower,
		// we need to handle the transition properly like clearing
		// the cache. It is better to exit for now.
		// kube will restart and this will become a follower.
		if hacontroller.manageDBServers {
			// Stop ovn-northd and then exit.
			_, _, _ = util.RunOVNNorthAppCtl("exit")
		}
		logrus.Infof("I (" + hacontroller.nodeName + ") am no longer a leader. Exiting")
		os.Exit(1)
	}

	hacontrollerNewLeader := func(nodeName string) {
		logrus.Infof(nodeName + " is the new leader")
		wasLeader := hacontroller.isLeader

		if hacontroller.nodeName == nodeName {
			// Configure as leader.
			logrus.Infof(" I (" + hacontroller.nodeName + ") won the election. In active mode")
			err = hacontroller.ConfigureAsActive(nodeName)
			if err != nil {
				logrus.Errorf(err.Error())
				if hacontroller.manageDBServers {
					// Stop ovn-northd before panicing.
					_, _, _ = util.RunOVNNorthAppCtl("exit")
				}
				panic(err.Error())
			}
			hacontroller.isLeader = true
		} else if wasLeader {
			hacontrollerOnStoppedLeading()
			// should not be reached
			panic("This should not happen.")
		} else {
			// Configure as standby.
			logrus.Infof(" I (" + hacontroller.nodeName + ") lost the election. In Standby mode")
			ep, er := hacontroller.ovnController.kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
			if er == nil {
				er = hacontroller.ConfigureAsStandby(ep)
				if er != nil {
					logrus.Errorf(er.Error())
					if hacontroller.manageDBServers {
						// Stop ovn-northd and then exit
						_, _, _ = util.RunOVNNorthAppCtl("exit")
					}
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
			OnStoppedLeading: hacontrollerOnStoppedLeading,
			OnNewLeader:      hacontrollerNewLeader,
		},
	}

	//go leaderelection.RunOrDie(context.Background(), lec)
	hacontroller.leaderElector, err = leaderelection.NewLeaderElector(lec)
	if err != nil {
		return err
	}

	go hacontroller.leaderElector.Run(context.Background())

	return hacontroller.WatchOvnDbEndpoints()
}

// ConfigureAsActive configures the node as active.
func (hacontroller *HAMasterController) ConfigureAsActive(masterNodeName string) error {
	if hacontroller.manageDBServers {
		// Step 1: Update the ovnkube-db endpoints with invalid Ip.
		// Step 2: Promote OVN DB servers to become active
		// Step 3: Make sure that ovn-northd has done one round of
		//         flow computation.
		// Step 4: Update the ovnkube-db endpoints with the new master Ip.

		// Find the endpoint for the service
		ep, err := hacontroller.ovnController.kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
		if err != nil {
			ep = nil
		}
		err = hacontroller.updateOvnDbEndpoints(ep, true)
		if err != nil {
			logrus.Errorf("%s Endpoint create/update failed", ovnkubeDbEp)
			return err
		}

		// Promote the OVN DB servers
		err = hacontroller.PromoteOVNDbs(ovnNorthDbPort, ovnSouthDbPort)
		if err != nil {
			logrus.Errorf("Promoting OVN ovsdb-servers to active failed")
			return err
		}

		// Wait for ovn-northd sync up
		err = hacontroller.syncOvnNorthd()
		if err != nil {
			logrus.Errorf("Waiting for ovn-northd to sync failed: %v", err)
			return err
		}

		ep, err = hacontroller.ovnController.kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp)
		if err != nil {
			// This should not happen.
			ep = nil
		}
		err = hacontroller.updateOvnDbEndpoints(ep, false)
		if err != nil {
			logrus.Errorf("%s Endpount create/update failed", ovnkubeDbEp)
			return err
		}
	}

	// run the cluster controller to init the master
	err := hacontroller.ovnController.StartClusterMaster(hacontroller.nodeName)
	if err != nil {
		return err
	}

	return hacontroller.ovnController.Run()
}

//updateOvnDbEndpoints Updates the ovnkube-db endpoints. Should be called
// only if ovnkube-master is leader. This function will create the ovnkube-db endpoints
// if it doesn't exist.
func (hacontroller *HAMasterController) updateOvnDbEndpoints(ep *kapi.Endpoints, configureInvalidIP bool) error {

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
				Namespace:   config.Kubernetes.OVNConfigNamespace,
				Name:        ovnkubeDbEp,
				Annotations: map[string]string{haMasterLeader: hacontroller.nodeName},
			},
			Subsets: epSubsets,
		}

		_, err = hacontroller.ovnController.kube.CreateEndpoint(config.Kubernetes.OVNConfigNamespace, &ovndbEp)
		if err != nil {
			logrus.Errorf("%s Endpount Create failed", ovnkubeDbEp)
		}
	} else {
		logrus.Debugf("updateOvnDbEndpoints : Updating the endpoint")
		ovndbEp := ep.DeepCopy()
		ovndbEp.Subsets = epSubsets
		ovndbEp.Annotations[haMasterLeader] = hacontroller.nodeName
		_, err := hacontroller.ovnController.kube.UpdateEndpoint(config.Kubernetes.OVNConfigNamespace, ovndbEp)
		if err != nil {
			logrus.Errorf("%s Endpount Update failed", ovnkubeDbEp)
		}
	}
	return err
}

// ConfigureAsStandby configures the node as standby
func (hacontroller *HAMasterController) ConfigureAsStandby(ep *kapi.Endpoints) error {
	if !hacontroller.manageDBServers {
		// Nothing to do if not managing db servers.
		return nil
	}

	// Get the master ip
	masterIPList, sbDBPort, nbDBPort, err := util.ExtractDbRemotesFromEndpoint(ep)
	if err != nil {
		// The db remotes are invalid. Return without doing anything.
		// Once master updates the endpoints properly we will be notified.
		logrus.Infof("ConfigureAsStandby : error in extracting DbRemotes From Endpoint")
		return nil
	}

	logrus.Infof("ConfigureAsStandby: New leader IP is : [%s]", masterIPList[0])

	activeServerOutOfSync := func(northbound bool, masterIP string, port int32) (bool, error) {
		var stdout, detail string
		var err error

		if northbound {
			detail = "northbound"
			stdout, _, err = util.RunOVNNBAppCtl("ovsdb-server/get-active-ovsdb-server")
		} else {
			detail = "southbound"
			stdout, _, err = util.RunOVNSBAppCtl("ovsdb-server/get-active-ovsdb-server")
		}
		if err != nil {
			logrus.Errorf("Getting  active-ovsdb-server of %s ovsdb-server failed", detail)
			return true, err
		}

		s := strings.Split(stdout, ":")
		if len(s) != 3 {
			return true, nil
		}

		if s[0] != "tcp" || s[1] != masterIP || s[2] != strconv.Itoa(int(port)) {
			return true, nil
		}

		return false, nil
	}

	outOfSync, err := activeServerOutOfSync(true, masterIPList[0], nbDBPort)
	if err != nil {
		return err
	}

	if !outOfSync {
		outOfSync, err = activeServerOutOfSync(false, masterIPList[0], sbDBPort)
		if err != nil || !outOfSync {
			return err
		}
	}

	logrus.Debugf("ConfigureAsStandby : active server out of sync..Setting the new active server to : " + masterIPList[0])
	err = hacontroller.DemoteOVNDbs(masterIPList[0], nbDBPort, sbDBPort)
	if err != nil {
		logrus.Errorf("Demoting OVN ovsdb-servers to standby failed")
	}
	return err
}

func (hacontroller *HAMasterController) validateOvnDbEndpoints(ep *kapi.Endpoints) bool {
	if ep.Name != ovnkubeDbEp {
		return false
	}

	leader, present := ep.Annotations[haMasterLeader]
	if !present || leader != hacontroller.leaderElector.GetLeader() {
		return false
	}

	masterIPList, sbDBPort, nbDBPort, err := util.ExtractDbRemotesFromEndpoint(ep)
	if err != nil {
		return false
	}

	if masterIPList[0] != config.Kubernetes.PodIP ||
		sbDBPort != ovnSouthDbPort || nbDBPort != ovnNorthDbPort {
		return false
	}

	return true
}

// WatchOvnDbEndpoints watches the ovnkube-db end point
func (hacontroller *HAMasterController) WatchOvnDbEndpoints() error {
	HandleOvnDbEpUpdate := func(ep *kapi.Endpoints) {
		if ep.Name != ovnkubeDbEp {
			return
		}
		if hacontroller.leaderElector.GetLeader() == "" {
			// If no leader is elected yet don't handle the endpoint updates.
			// This can happen for the first time when ovnkube is started.
			return
		}
		if hacontroller.leaderElector.IsLeader() {
			if !hacontroller.validateOvnDbEndpoints(ep) {
				_ = hacontroller.updateOvnDbEndpoints(ep, false)
			}
		} else {
			err := hacontroller.ConfigureAsStandby(ep)
			if err != nil {
				logrus.Errorf(err.Error())
				panic(err.Error())
			}
		}
	}

	_, err := hacontroller.ovnController.watchFactory.AddFilteredEndpointsHandler(config.Kubernetes.OVNConfigNamespace,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ep := obj.(*kapi.Endpoints)
				HandleOvnDbEpUpdate(ep)
			},
			UpdateFunc: func(old, new interface{}) {
				epNew := new.(*kapi.Endpoints)
				HandleOvnDbEpUpdate(epNew)
			},
			DeleteFunc: func(obj interface{}) {
				ep := obj.(*kapi.Endpoints)
				if ep.Name == ovnkubeDbEp && hacontroller.leaderElector.IsLeader() {
					_ = hacontroller.updateOvnDbEndpoints(nil, false)
				}
			},
		}, nil)
	return err
}

//PromoteOVNDbs promotes the OVN Db servers and resumes the ovn-northd.
func (hacontroller *HAMasterController) PromoteOVNDbs(nbDBPort, sbDBPort int32) error {
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
	target := "ptcp:" + strconv.Itoa(int(nbDBPort)) + ":" + config.Kubernetes.PodIP
	_, _, err = util.RunOVNNBAppCtl("ovsdb-server/add-remote", target)
	if err != nil {
		logrus.Errorf("Adding remote [%s] to NB ovsdb-server failed", target)
		return err
	}

	target = "ptcp:" + strconv.Itoa(int(sbDBPort)) + ":" + config.Kubernetes.PodIP
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
func (hacontroller *HAMasterController) DemoteOVNDbs(masterIP string, nbDBPort, sbDBPort int32) error {
	_, _, err := util.RunOVNCtl("demote_ovnnb", "--db-nb-sync-from-addr="+masterIP,
		"--db-nb-sync-from-port="+strconv.Itoa(int(nbDBPort)))
	if err != nil {
		logrus.Errorf("Demoting NB ovsdb-server failed")
		return err
	}

	_, _, err = util.RunOVNCtl("demote_ovnsb", "--db-sb-sync-from-addr="+masterIP,
		"--db-sb-sync-from-port="+strconv.Itoa(int(sbDBPort)))

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

func (hacontroller *HAMasterController) syncOvnNorthd() error {
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
