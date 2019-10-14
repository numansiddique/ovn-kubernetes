package ovn

import (
	"context"
	"fmt"
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
	haLeaderLockName = "ovn-kubernetes-master"
	ovnkubeDbEp      = "ovnkube-db"
	haMasterLeader   = "k8s.ovn.org.ovnkube-master-leader"
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

// StartHAMasterCluster runs the replication controller
func (hacontroller *HAMasterController) StartHAMasterCluster() error {

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

	HAClusterOnStartedLeading := func(ctx context.Context) {
		logrus.Infof(" I (%s) won the election. In active mode", hacontroller.nodeName)
		err = hacontroller.ConfigureAsActive(hacontroller.nodeName)
		if err != nil {
			if hacontroller.manageDBServers {
				// Stop ovn-northd before panicing.
				_, _, _ = util.RunOVNNorthAppCtl("exit")
			}
			panic(err.Error())
		}
		hacontroller.isLeader = true
	}

	HAClusterOnStoppedLeading := func() {
		// This node was leader and it lost the election.
		// Whenever the node transitions from leader to follower,
		// we need to handle the transition properly like clearing
		// the cache. It is better to exit for now.
		// kube will restart and this will become a follower.
		if hacontroller.manageDBServers {
			// Stop ovn-northd and then exit.
			_, _, _ = util.RunOVNNorthAppCtl("exit")
		}
		logrus.Infof("I (%s) am no longer a leader. Exiting", hacontroller.nodeName)
		os.Exit(1)
	}

	HAClusterOnNewLeader := func(nodeName string) {
		if nodeName != hacontroller.nodeName {
			logrus.Infof(" I (%s) lost the election to %s. In Standby mode", hacontroller.nodeName, nodeName)
		}
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: time.Duration(config.MasterHA.HAElectionLeaseDuration) * time.Second,
		RenewDeadline: time.Duration(config.MasterHA.HAElectionRenewDeadline) * time.Second,
		RetryPeriod:   time.Duration(config.MasterHA.HAElectionRetryPeriod) * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: HAClusterOnStartedLeading,
			OnStoppedLeading: HAClusterOnStoppedLeading,
			OnNewLeader:      HAClusterOnNewLeader,
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
		// Step 1: Promote OVN DB servers to become active
		// Step 2: Make sure that ovn-northd has done one round of
		//         flow computation.
		// Step 3: Update the ovnkube-db endpoints with the new master Ip.

		// Promote the OVN DB servers
		err := hacontroller.PromoteOVNDbs(config.MasterHA.NbPort, config.MasterHA.SbPort)
		if err != nil {
			return fmt.Errorf("promoting OVN ovsdb-servers to active failed: %v", err.Error())
		}

		// Wait for ovn-northd sync up
		err = hacontroller.syncOvnNorthd()
		if err != nil {
			return fmt.Errorf("syncing ovn-northd failed: %v", err.Error())
		}

		if ep, err := hacontroller.ovnController.kube.GetEndpoint(config.Kubernetes.OVNConfigNamespace, ovnkubeDbEp); err != nil {
			if err := hacontroller.createOvnDbEndpoints(); err != nil {
				return fmt.Errorf("%s endpoint create failed: %v", ovnkubeDbEp, err.Error())
			}
		} else {
			if err := hacontroller.updateOvnDbEndpoints(ep); err != nil {
				return fmt.Errorf("%s endpoint update failed: %v", ovnkubeDbEp, err.Error())
			}
		}
	}

	// run the cluster controller to init the master
	err := hacontroller.ovnController.StartClusterMaster(hacontroller.nodeName)
	if err != nil {
		return err
	}

	return hacontroller.ovnController.Run()
}

func (hacontroller *HAMasterController) generateOvnDbEndpoint() *v1.Endpoints {
	ep := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   config.Kubernetes.OVNConfigNamespace,
			Name:        ovnkubeDbEp,
			Annotations: map[string]string{haMasterLeader: hacontroller.nodeName},
		},
		Subsets: []v1.EndpointSubset{
			{
				Addresses: []v1.EndpointAddress{
					{IP: config.Kubernetes.PodIP},
				},
				Ports: []v1.EndpointPort{
					{
						Name: "north",
						Port: int32(config.MasterHA.NbPort),
					},
					{
						Name: "south",
						Port: int32(config.MasterHA.SbPort),
					},
				},
			},
		},
	}
	return ep
}

func (hacontroller *HAMasterController) createOvnDbEndpoints() error {
	logrus.Debugf("creating the endpoint")
	ovndbEp := hacontroller.generateOvnDbEndpoint()
	if _, err := hacontroller.ovnController.kube.CreateEndpoint(config.Kubernetes.OVNConfigNamespace, ovndbEp); err != nil {
		return fmt.Errorf("%s endpoint create failed: %v", ovnkubeDbEp, err.Error())
	}
	return nil
}

//updateOvnDbEndpoints Updates the ovnkube-db endpoints. Should be called
// only if ovnkube-master is leader. This function will create the ovnkube-db endpoints
// if it doesn't exist.
func (hacontroller *HAMasterController) updateOvnDbEndpoints(ep *kapi.Endpoints) error {
	logrus.Debugf("updating the endpoint")
	ovndbEp := ep.DeepCopy()
	ovndbEp = hacontroller.generateOvnDbEndpoint()
	if _, err := hacontroller.ovnController.kube.UpdateEndpoint(config.Kubernetes.OVNConfigNamespace, ovndbEp); err != nil {
		return fmt.Errorf("%s endpoint update failed: %v", ovnkubeDbEp, err.Error())
	}
	return nil
}

// ConfigureAsStandby configures the node as standby
func (hacontroller *HAMasterController) ConfigureAsStandby(ep *kapi.Endpoints) error {
	if !hacontroller.manageDBServers {
		// Nothing to do if not managing db servers.
		return nil
	}

	// Get the master ip
	masterIP, sbDBPort, nbDBPort, err := util.ExtractDbRemotesFromEndpoint(ep)
	if err != nil {
		return fmt.Errorf("error extracting DbRemotes From Endpoint: %v", err.Error())
	}

	logrus.Infof("new leader IP is : [%s]", masterIP)

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
			return true, fmt.Errorf("getting active-ovsdb-server of %s ovsdb-server failed: %s", detail, err.Error())
		}

		s := strings.Split(stdout, ":")
		if len(s) != 3 {
			return true, nil
		}

		if s[0] != string(config.OvnDBSchemeTCP) || s[1] != masterIP || s[2] != strconv.Itoa(int(port)) {
			return true, nil
		}

		return false, nil
	}

	outOfSync, err := activeServerOutOfSync(true, masterIP, nbDBPort)
	if err != nil {
		return err
	}

	if !outOfSync {
		outOfSync, err = activeServerOutOfSync(false, masterIP, sbDBPort)
		if err != nil || !outOfSync {
			return err
		}
	}

	logrus.Debugf("active server out of sync. Setting the new active server to: %s ", masterIP)

	err = hacontroller.DemoteOVNDbs(masterIP, int(nbDBPort), int(sbDBPort))
	if err != nil {
		return fmt.Errorf("demoting OVN ovsdb-servers to standby failed")
	}

	return nil
}

func (hacontroller *HAMasterController) isValidOVNDBEndpoints(ep *kapi.Endpoints) (bool, error) {
	if ep.Name != ovnkubeDbEp {
		return false, fmt.Errorf("invalid name: %s ", ep.Name)
	}

	if hacontroller.leaderElector.GetLeader() == "" {
		return false, fmt.Errorf("no leader elected yet")
	}

	leader, present := ep.Annotations[haMasterLeader]
	if !present || leader != hacontroller.leaderElector.GetLeader() {
		return false, fmt.Errorf("invalid leader annotation: ep-leader: %s, real leader: %s", leader, hacontroller.leaderElector.GetLeader())
	}

	_, sbDBPort, nbDBPort, err := util.ExtractDbRemotesFromEndpoint(ep)
	if err != nil {
		return false, fmt.Errorf(err.Error())
	}

	if sbDBPort != int32(config.MasterHA.SbPort) || nbDBPort != int32(config.MasterHA.NbPort) {
		return false, fmt.Errorf("ports not adhering to config: sb-port: %v nb-port: %v", sbDBPort, nbDBPort)
	}

	return true, nil
}

// WatchOvnDbEndpoints watches the ovnkube-db end point
func (hacontroller *HAMasterController) WatchOvnDbEndpoints() error {
	HandleOvnDbEpUpdate := func(ep *kapi.Endpoints) error {
		if isValid, err := hacontroller.isValidOVNDBEndpoints(ep); !isValid {
			return fmt.Errorf("invalid endpoint: %s", err.Error())
		}
		if hacontroller.leaderElector.IsLeader() {
			if err := hacontroller.updateOvnDbEndpoints(ep); err != nil {
				return err
			}
		} else if err := hacontroller.ConfigureAsStandby(ep); err != nil {
			return err
		}
		return nil
	}

	_, err := hacontroller.ovnController.watchFactory.AddFilteredEndpointsHandler(config.Kubernetes.OVNConfigNamespace,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ep := obj.(*kapi.Endpoints)
				if err := HandleOvnDbEpUpdate(ep); err != nil {
					logrus.Errorf(err.Error())
				}
			},
			UpdateFunc: func(old, new interface{}) {
				epNew := new.(*kapi.Endpoints)
				if err := HandleOvnDbEpUpdate(epNew); err != nil {
					logrus.Errorf(err.Error())
				}
			},
			DeleteFunc: func(obj interface{}) {
				ep := obj.(*kapi.Endpoints)
				if ep.Name == ovnkubeDbEp && hacontroller.leaderElector.IsLeader() {
					if err := hacontroller.createOvnDbEndpoints(); err != nil {
						logrus.Errorf(err.Error())
					}
				}
			},
		}, nil, false)

	return err
}

//PromoteOVNDbs promotes the OVN Db servers and resumes the ovn-northd.
func (hacontroller *HAMasterController) PromoteOVNDbs(nbDBPort, sbDBPort int) error {
	_, _, err := util.RunOVNCtl("promote_ovnnb")
	if err != nil {
		return fmt.Errorf("ovnnb execution failed: %v", err.Error())
	}

	_, _, err = util.RunOVNCtl("promote_ovnsb")
	if err != nil {
		return fmt.Errorf("ovnsb execution failed: %v", err.Error())
	}

	// Configure OVN dbs to listen on the ovnkube-pod-ip
	target := "ptcp:" + strconv.Itoa(nbDBPort) + ":" + config.Kubernetes.PodIP
	_, _, err = util.RunOVNNBAppCtl("ovsdb-server/add-remote", target)
	if err != nil {
		return fmt.Errorf("adding remote [%s] to NB ovsdb-server failed: %v", target, err.Error())
	}

	target = "ptcp:" + strconv.Itoa(sbDBPort) + ":" + config.Kubernetes.PodIP
	_, _, err = util.RunOVNSBAppCtl("ovsdb-server/add-remote", target)
	if err != nil {
		return fmt.Errorf("adding remote [%s] to SB ovsdb-server failed: %v", target, err.Error())
	}

	// Ignore the error from this command for now as the patch in OVN
	// to add the pause/resume support is not yet merged.
	//TODO: Handle the err properly when ovn-northd supports pause/resume
	_, _, _ = util.RunOVNNorthAppCtl("resume")
	return nil
}

//DemoteOVNDbs demotes the OVN Db servers and configure them to connect
//to the new master and pauses the ovn-northd.
func (hacontroller *HAMasterController) DemoteOVNDbs(masterIP string, nbDBPort, sbDBPort int) error {
	_, _, err := util.RunOVNCtl("demote_ovnnb", "--db-nb-sync-from-addr="+masterIP,
		"--db-nb-sync-from-port="+strconv.Itoa(nbDBPort))
	if err != nil {
		return fmt.Errorf("demoting NB ovsdb-server failed: %v", err.Error())
	}

	_, _, err = util.RunOVNCtl("demote_ovnsb", "--db-sb-sync-from-addr="+masterIP,
		"--db-sb-sync-from-port="+strconv.Itoa(sbDBPort))

	if err != nil {
		return fmt.Errorf("demoting SB ovsdb-server failed: %v", err.Error())
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
		return fmt.Errorf("error getting NB_Global's nb_cfg column: %v", err.Error())
	}

	nbNbCfg, _ := strconv.Atoi(stdout)

	stdout, _, err = util.RunOVNSbctl("--bare", "--columns", "nb_cfg", "list", "SB_Global")
	if err != nil {
		return fmt.Errorf("error getting SB_Global's nb_cfg column: %v", err.Error())
	}

	sbNbCfg, _ := strconv.Atoi(stdout)

	nbNbCfg++
	if nbNbCfg == sbNbCfg {
		nbNbCfg++
	}

	nbCfgValue := "nb_cfg=" + strconv.Itoa(nbNbCfg)
	_, _, err = util.RunOVNNbctl("set", "NB_Global", ".", nbCfgValue)
	if err != nil {
		return fmt.Errorf("error setting NB_Global's nb_cfg column: %v", err.Error())
	}

	if err = wait.PollImmediate(500*time.Millisecond, 30*time.Second, func() (bool, error) {
		stdout, _, err = util.RunOVNSbctl("--bare", "--columns", "nb_cfg", "list", "SB_Global")
		if err != nil {
			return false, fmt.Errorf("error getting SB_Global's nb_cfg column: %v", err.Error())
		}

		sbNbCfg, _ := strconv.Atoi(stdout)
		if nbNbCfg == sbNbCfg {
			return true, nil
		}

		return false, nil
	}); err != nil {
		return fmt.Errorf("error getting the correct nb_cfg value in SB_Global table: %v", err.Error())
	}
	return nil
}
