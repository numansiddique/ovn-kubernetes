package cluster

import (
	"fmt"
	"net"
	"strconv"

	"github.com/openshift/origin/pkg/util/netutils"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	kapi "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// OvnClusterController is the object holder for utilities meant for cluster management
type OvnClusterController struct {
	Kube                      kube.Interface
	watchFactory              *factory.WatchFactory
	masterSubnetAllocatorList []*netutils.SubnetAllocator
	nodeName                  string

	TCPLoadBalancerUUID string
	UDPLoadBalancerUUID string
}

type ClusterController interface {
	StartMaster() error
	StartNode() error
}

const (
	// OvnHostSubnet is the constant string representing the annotation key
	OvnHostSubnet = "ovn_host_subnet"
	// OvnClusterRouter is the name of the distributed router
	OvnClusterRouter = "ovn_cluster_router"
)

// NewClusterController creates a new controller for IP subnet allocation to
// a given resource type (either Namespace or Node)
func NewClusterController(kubeClient kubernetes.Interface, wf *factory.WatchFactory, nodeName string) *OvnClusterController {
	return &OvnClusterController{
		Kube:         &kube.Kube{KClient: kubeClient},
		watchFactory: wf,
		nodeName:     nodeName,
	}
}

func setupOVNNode(nodeName string) error {
	// Tell ovn-*bctl how to talk to the database
	for _, auth := range []config.OvnAuthConfig{config.OvnNorth, config.OvnSouth} {
		if err := auth.SetDBAuth(); err != nil {
			return err
		}
	}

	var err error

	nodeIP := config.Default.EncapIP
	if nodeIP == "" {
		nodeIP, err = netutils.GetNodeIP(nodeName)
		if err != nil {
			return fmt.Errorf("failed to obtain local IP from hostname %q: %v", nodeName, err)
		}
	} else {
		if ip := net.ParseIP(nodeIP); ip == nil {
			return fmt.Errorf("invalid encapsulation IP provided %q", nodeIP)
		}
	}

	_, stderr, err := util.RunOVSVsctl("set",
		"Open_vSwitch",
		".",
		fmt.Sprintf("external_ids:ovn-encap-type=%s", config.Default.EncapType),
		fmt.Sprintf("external_ids:ovn-encap-ip=%s", nodeIP),
		fmt.Sprintf("external_ids:ovn-remote-probe-interval=%d",
			config.Default.InactivityProbe),
		fmt.Sprintf("external_ids:hostname=\"%s\"", nodeName),
	)
	if err != nil {
		return fmt.Errorf("error setting OVS external IDs: %v\n  %q", err, stderr)
	}
	// If EncapPort is not the default tell sbdb to use specified port.
	if config.Default.EncapPort != config.DefaultEncapPort {
		systemID, err := util.GetNodeChassisID()
		if err != nil {
			return err
		}
		_, stderr, errSet := util.RunOVNSbctl("set", "encap", systemID,
			fmt.Sprintf("options:dst_port=%d", config.Default.EncapPort),
		)
		if errSet != nil {
			return fmt.Errorf("error setting OVS encap-port: %v\n  %q", errSet, stderr)
		}
	}
	return nil
}

func setupOVNMaster(nodeName string) error {
	// Configure both server and client of OVN databases, since master uses both
	for _, auth := range []config.OvnAuthConfig{config.OvnNorth, config.OvnSouth} {
		if err := auth.SetDBAuth(); err != nil {
			return err
		}
	}
	return nil
}

func validateOVNConfigEndpoint(ep *kapi.Endpoints) bool {
	return len(ep.Subsets) == 1 && len(ep.Subsets[0].Ports) == 2 && len(ep.Subsets[0].Addresses) > 0
}

func extractDbRemotesFromEndpoint(ep *kapi.Endpoints) ([]string, string, string, error) {
	var nbDBPort string
	var sbDBPort string
	var masterIPList []string

	if !validateOVNConfigEndpoint(ep) {
		return masterIPList, nbDBPort, sbDBPort, fmt.Errorf("endpoint %s is not in the right format to configure OVN", ep.Name)
	}

	for _, ovnDB := range ep.Subsets[0].Ports {
		if ovnDB.Name == "south" {
			sbDBPort = strconv.Itoa(int(ovnDB.Port))
		} else if ovnDB.Name == "north" {
			nbDBPort = strconv.Itoa(int(ovnDB.Port))
		}
	}
	for _, address := range ep.Subsets[0].Addresses {
		masterIPList = append(masterIPList, address.IP)
	}

	return masterIPList, sbDBPort, nbDBPort, nil
}
