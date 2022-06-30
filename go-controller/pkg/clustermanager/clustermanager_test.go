package clustermanager

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	hotypes "github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	egressfirewallfake "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressfirewall/v1/apis/clientset/versioned/fake"
	egressipfake "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressip/v1/apis/clientset/versioned/fake"
	egressqosfake "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressqos/v1/apis/clientset/versioned/fake"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/urfave/cli/v2"
	kapi "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

func TestClusterManager_allocateNodeSubnets(t *testing.T) {
	tests := []struct {
		name          string
		networkRanges []string
		networkLen    int
		configIPv4    bool
		configIPv6    bool
		node          *kapi.Node
		// to be converted during the test to []*net.IPNet
		wantStr   []string
		allocated int
		wantErr   bool
	}{
		{
			name:          "new node, IPv4 only cluster",
			networkRanges: []string{"172.16.0.0/16"},
			networkLen:    24,
			configIPv4:    true,
			configIPv6:    false,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "testnode",
					Annotations: map[string]string{},
				},
			},
			wantStr:   []string{"172.16.0.0/24"},
			allocated: 1,
			wantErr:   false,
		},
		{
			name:          "new node, IPv6 only cluster",
			networkRanges: []string{"2001:db2::/56"},
			networkLen:    64,
			configIPv4:    false,
			configIPv6:    true,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "testnode",
					Annotations: map[string]string{},
				},
			},
			wantStr:   []string{"2001:db2::/64"},
			allocated: 1,
			wantErr:   false,
		},
		{
			name:          "existing annotated node, IPv4 only cluster",
			networkRanges: []string{"172.16.0.0/16"},
			networkLen:    24,
			configIPv4:    true,
			configIPv6:    false,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testnode",
					Annotations: map[string]string{
						"k8s.ovn.org/node-subnets": `{"default": "172.16.8.0/24"}`,
					},
				},
			},
			wantStr:   []string{"172.16.8.0/24"},
			allocated: 0,
			wantErr:   false,
		},
		{
			name:          "existing annotated node, IPv6 only cluster",
			networkRanges: []string{"2001:db2::/56"},
			networkLen:    64,
			configIPv4:    false,
			configIPv6:    true,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testnode",
					Annotations: map[string]string{
						"k8s.ovn.org/node-subnets": `{"default": "2001:db2:1:2:3:4::/64"}`,
					}},
			},
			wantStr:   []string{"2001:db2:1:2:3:4::/64"},
			allocated: 0,
			wantErr:   false,
		},
		{
			name:          "new node, dual stack cluster",
			networkRanges: []string{"172.16.0.0/16", "2000::/12"},
			networkLen:    24,
			configIPv4:    true,
			configIPv6:    true,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "testnode",
					Annotations: map[string]string{},
				},
			},
			wantStr:   []string{"172.16.0.0/24", "2000::/24"},
			allocated: 2,
			wantErr:   false,
		},
		{
			name:          "annotated node, dual stack cluster",
			networkRanges: []string{"172.16.0.0/16", "2000::/12"},
			networkLen:    24,
			configIPv4:    true,
			configIPv6:    true,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testnode",
					Annotations: map[string]string{
						"k8s.ovn.org/node-subnets": `{"default": ["172.16.5.0/24","2000:2::/24"]}`,
					},
				},
			},
			wantStr:   []string{"172.16.5.0/24", "2000:2::/24"},
			allocated: 0,
			wantErr:   false,
		},
		{
			name:          "single IPv4 to dual stack cluster",
			networkRanges: []string{"172.16.0.0/16", "2000::/12"},
			networkLen:    24,
			configIPv4:    true,
			configIPv6:    true,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testnode",
					Annotations: map[string]string{
						"k8s.ovn.org/node-subnets": `{"default": "172.16.5.0/24"}`,
					},
				},
			},
			wantStr:   []string{"172.16.5.0/24", "2000::/24"},
			allocated: 1,
			wantErr:   false,
		},
		{
			name:          "single IPv6 to dual stack cluster",
			networkRanges: []string{"172.16.0.0/16", "2000:1::/12"},
			networkLen:    24,
			configIPv4:    true,
			configIPv6:    true,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testnode",
					Annotations: map[string]string{
						"k8s.ovn.org/node-subnets": `{"default": "2000:1::/24"}`,
					},
				},
			},
			wantStr:   []string{"2000::/24", "172.16.0.0/24"},
			allocated: 1,
			wantErr:   false,
		},
		{
			name:          "dual stack cluster to single IPv4",
			networkRanges: []string{"172.16.0.0/16"},
			networkLen:    24,
			configIPv4:    true,
			configIPv6:    false,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testnode",
					Annotations: map[string]string{
						"k8s.ovn.org/node-subnets": `{"default": ["172.16.5.0/24","2000:2::/24"]}`,
					},
				},
			},
			wantStr:   []string{"172.16.5.0/24"},
			allocated: 0,
			wantErr:   false,
		},
		{
			name:          "dual stack cluster to single IPv6",
			networkRanges: []string{"2001:db2::/56"},
			networkLen:    64,
			configIPv4:    false,
			configIPv6:    true,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testnode",
					Annotations: map[string]string{
						"k8s.ovn.org/node-subnets": `{"default": ["172.16.5.0/24","2001:db2:1:2:3:4::/64"]}`,
					},
				},
			},
			wantStr:   []string{"2001:db2:1:2:3:4::/64"},
			allocated: 0,
			wantErr:   false,
		},
		{
			name:          "new node, OVN wrong configuration: IPv4 only cluster but IPv6 range",
			networkRanges: []string{"2001:db2::/64"},
			networkLen:    112,
			configIPv4:    true,
			configIPv6:    false,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "testnode",
					Annotations: map[string]string{},
				},
			},
			wantStr:   nil,
			allocated: 0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create cluster config
			config.IPv4Mode = tt.configIPv4
			config.IPv6Mode = tt.configIPv6
			// create fake OVN controller
			stopChan := make(chan struct{})
			defer close(stopChan)
			kubeFakeClient := fake.NewSimpleClientset()
			egressFirewallFakeClient := &egressfirewallfake.Clientset{}
			egressIPFakeClient := &egressipfake.Clientset{}
			egressQoSFakeClient := &egressqosfake.Clientset{}
			fakeClient := &util.OVNClientset{
				KubeClient:           kubeFakeClient,
				EgressIPClient:       egressIPFakeClient,
				EgressFirewallClient: egressFirewallFakeClient,
				EgressQoSClient:      egressQoSFakeClient,
			}
			f, err := factory.NewClusterManagerWatchFactory(fakeClient)
			if err != nil {
				t.Fatalf("Error creating master watch factory: %v", err)
			}
			if err := f.Start(); err != nil {
				t.Fatalf("Error starting master watch factory: %v", err)
			}

			clusterManager := NewClusterManager(fakeClient, f, stopChan,
				record.NewFakeRecorder(0))

			// configure the cluster allocators
			for _, subnetString := range tt.networkRanges {
				_, subnet, err := net.ParseCIDR(subnetString)
				if err != nil {
					t.Fatalf("Error parsing subnet %s", subnetString)
				}
				clusterManager.clusterSubnetAllocator.AddNetworkRange(subnet, tt.networkLen)
			}
			// test network allocation works correctly
			got, allocated, err := clusterManager.allocateNodeSubnets(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("Controller.addNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			var want []*net.IPNet
			for _, netStr := range tt.wantStr {
				_, ipnet, err := net.ParseCIDR(netStr)
				if err != nil {
					t.Fatalf("Error parsing subnet %s", netStr)
				}
				want = append(want, ipnet)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("clusterManager.allocateNodeSubnets() = %v, want %v", got, want)
			}

			if len(allocated) != tt.allocated {
				t.Errorf("Expected %d subnets allocated, received %d", tt.allocated, len(allocated))
			}
		})
	}
}

var _ = ginkgo.Describe("Cluster Manager operations", func() {
	var (
		app      *cli.App
		f        *factory.WatchFactory
		stopChan chan struct{}
		wg       *sync.WaitGroup
	)

	const (
		clusterIPNet             string = "10.1.0.0"
		clusterCIDR              string = clusterIPNet + "/16"
		joinSubnetCIDR           string = "100.64.0.0/16/19"
		hybridOverlayClusterCIDR string = "11.1.0.0/16/24"
	)

	ginkgo.BeforeEach(func() {
		// Restore global default values before each testcase
		gomega.Expect(config.PrepareTestConfig()).To(gomega.Succeed())

		app = cli.NewApp()
		app.Name = "test"
		app.Flags = config.Flags
		stopChan = make(chan struct{})
		wg = &sync.WaitGroup{}
	})

	ginkgo.AfterEach(func() {
		close(stopChan)
		f.Shutdown()
		wg.Wait()
	})

	ginkgo.It("Cluster Manager Node subnet allocations, node id allocations and zone join subnet allocations", func() {

		app.Action = func(ctx *cli.Context) error {
			nodes := []v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node3",
					},
				}}
			kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
				Items: nodes,
			})
			egressFirewallFakeClient := &egressfirewallfake.Clientset{}
			egressIPFakeClient := &egressipfake.Clientset{}
			egressQoSFakeClient := &egressqosfake.Clientset{}
			fakeClient := &util.OVNClientset{
				KubeClient:           kubeFakeClient,
				EgressIPClient:       egressIPFakeClient,
				EgressFirewallClient: egressFirewallFakeClient,
				EgressQoSClient:      egressQoSFakeClient,
			}

			_, err := config.InitConfig(ctx, nil, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			config.Kubernetes.HostNetworkNamespace = ""

			f, err = factory.NewClusterManagerWatchFactory(fakeClient)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = f.Start()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			clusterManager := NewClusterManager(fakeClient, f, stopChan,
				record.NewFakeRecorder(0))
			gomega.Expect(clusterManager).NotTo(gomega.BeNil())
			for _, clusterEntry := range config.HybridOverlay.ClusterSubnets {
				clusterManager.AddHybridOverlaySubnetNetworkRange(clusterEntry.CIDR, clusterEntry.HostSubnetLength)
			}

			for _, clusterEntry := range config.Default.ClusterSubnets {
				clusterManager.AddClusterSubnetNetworkRange(clusterEntry.CIDR, clusterEntry.HostSubnetLength)
			}
			for _, clusterEntry := range config.ClusterManager.ZoneJoinSubnets {
				clusterManager.AddJoinSubnetNetworkRange(clusterEntry.CIDR, clusterEntry.HostSubnetLength)
			}
			gomega.Expect(clusterManager.WatchNodes()).To(gomega.Succeed())

			// Check that cluster manager has set the subnet annotation for each node.
			for _, n := range nodes {
				gomega.Eventually(func() ([]*net.IPNet, error) {
					updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
					if err != nil {
						return nil, err
					}

					return util.ParseNodeHostSubnetAnnotation(updatedNode)
				}, 2).Should(gomega.HaveLen(1))
			}

			for _, n := range nodes {
				gomega.Eventually(func() error {
					updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
					if err != nil {
						return err
					}

					nodeId, ok := updatedNode.Annotations[util.OvnNodeId]
					if !ok {
						return fmt.Errorf("expected node annotation for node %s to have node id allocated", n.Name)
					}

					_, err = strconv.Atoi(nodeId)
					if err != nil {
						return fmt.Errorf("expected node annotation for node %s to be an integer value, got %s", n.Name, nodeId)
					}

					_, ok = updatedNode.Annotations["k8s.ovn.org/ovn-node-transit-switch-port-ips"]
					if !ok {
						return fmt.Errorf("expected node annotation for node %s to have transit switch port ips allocated", n.Name)
					}

					transitSwitchIps, err := util.ParseNodeTransitSwitchPortAddresses(updatedNode)
					if err != nil {
						return fmt.Errorf("error parsing transit switch ip annotations for the node %s", n.Name)
					}

					if len(transitSwitchIps) < 1 {
						return fmt.Errorf("transit switch ips for node %s not allocated", n.Name)
					}
					return nil
				}).ShouldNot(gomega.HaveOccurred())
			}

			// Check that cluster manager has set the zone join subnet annotation for each node.
			for _, n := range nodes {
				gomega.Eventually(func() ([]*net.IPNet, error) {
					updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
					if err != nil {
						return nil, err
					}

					return util.ParseZoneJoinSubnetsAnnotation(updatedNode)
				}, 2).Should(gomega.HaveLen(1))
			}

			// Clear the subnet annotation of node 1 and make sure it is re-allocated by cluster manager.
			nodeAnnotator := kube.NewNodeAnnotator(&kube.Kube{kubeFakeClient, egressIPFakeClient, egressFirewallFakeClient, nil}, "node1")
			util.DeleteNodeHostSubnetAnnotation(nodeAnnotator)
			err = nodeAnnotator.Run()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Eventually(func() ([]*net.IPNet, error) {
				updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node1", metav1.GetOptions{})
				if err != nil {
					return nil, err
				}

				return util.ParseNodeHostSubnetAnnotation(updatedNode)
			}, 2).Should(gomega.HaveLen(1))

			node1, _ := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node1", metav1.GetOptions{})
			node1Id := node1.Annotations[util.OvnNodeId]

			// Clear the nodeId annotation from node 1.
			nodeAnnotations := node1.Annotations
			for k, v := range nodeAnnotations {
				nodeAnnotator.Set(k, v)
			}
			nodeAnnotator.Delete(util.OvnNodeId)
			err = nodeAnnotator.Run()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Eventually(func() error {
				updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node1", metav1.GetOptions{})
				if err != nil {
					return err
				}

				updatedNodeId, ok := updatedNode.Annotations[util.OvnNodeId]
				if !ok {
					return fmt.Errorf("expected node annotation for node - node1 to have node id allocated")
				}

				_, err = strconv.Atoi(updatedNodeId)
				if err != nil {
					return fmt.Errorf("expected node annotation for node - node1 to be an integer value, got %s", updatedNodeId)
				}

				gomega.Expect(node1Id).To(gomega.Equal(updatedNodeId))
				return nil
			}).ShouldNot(gomega.HaveOccurred())

			// Clear the transit switch port ip annotation from node 1.
			node1, _ = fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node1", metav1.GetOptions{})
			nodeAnnotations = node1.Annotations
			for k, v := range nodeAnnotations {
				nodeAnnotator.Set(k, v)
			}
			node1TransitSwitchIps := node1.Annotations["k8s.ovn.org/ovn-node-transit-switch-port-ips"]
			nodeAnnotator.Delete("k8s.ovn.org/ovn-node-transit-switch-port-ips")
			err = nodeAnnotator.Run()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Eventually(func() error {
				updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node1", metav1.GetOptions{})
				if err != nil {
					return err
				}

				updatedNode1TransitSwitchIps, ok := updatedNode.Annotations["k8s.ovn.org/ovn-node-transit-switch-port-ips"]
				if !ok {
					return fmt.Errorf("expected node annotation for node node1 to have transit switch port ips allocated")
				}

				transitSwitchIps, err := util.ParseNodeTransitSwitchPortAddresses(updatedNode)
				if err != nil {
					return fmt.Errorf("error parsing transit switch ip annotations for the node node1")
				}

				if len(transitSwitchIps) < 1 {
					return fmt.Errorf("transit switch ips for node node1 not allocated")
				}
				gomega.Expect(node1TransitSwitchIps).To(gomega.Equal(updatedNode1TransitSwitchIps))
				return nil
			}).ShouldNot(gomega.HaveOccurred())

			node1, err = fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node1", metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			globalZoneJoinSubnetForNode1 := node1.Annotations["k8s.ovn.org/zone-join-subnets"]

			node2, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node2", metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			globalZoneJoinSubnetForNode2 := node2.Annotations["k8s.ovn.org/zone-join-subnets"]
			gomega.Expect(globalZoneJoinSubnetForNode1).To(gomega.Equal(globalZoneJoinSubnetForNode2))

			// Change the zone of node 1.
			nodeAnnotations = node1.Annotations
			for k, v := range nodeAnnotations {
				nodeAnnotator.Set(k, v)
			}
			nodeAnnotator.Set("k8s.ovn.org/ovn-zone", "node1Zone")
			err = nodeAnnotator.Run()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Eventually(func() error {
				updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node1", metav1.GetOptions{})
				if err != nil {
					return err
				}

				updatedNode1JoinSubnetAnnotation, ok := updatedNode.Annotations["k8s.ovn.org/zone-join-subnets"]
				if !ok {
					return fmt.Errorf("expected node annotation for node node1 to have zone join subnets")
				}

				if updatedNode1JoinSubnetAnnotation == globalZoneJoinSubnetForNode1 {
					return fmt.Errorf("join subnet annotation for node node1 should have different subnet than the global zone")
				}
				joinSubnets, err := util.ParseZoneJoinSubnetsAnnotation(updatedNode)
				if err != nil {
					return fmt.Errorf("error parsing zone join subnet annotation for the node node1")
				}

				if len(joinSubnets) < 1 {
					return fmt.Errorf("zone join subnet annotation for node1 is empty")
				}
				return nil
			}).ShouldNot(gomega.HaveOccurred())

			// Reset the zone of node 1 to global
			nodeAnnotations = node1.Annotations
			for k, v := range nodeAnnotations {
				nodeAnnotator.Set(k, v)
			}
			nodeAnnotator.Set("k8s.ovn.org/ovn-zone", "global")
			err = nodeAnnotator.Run()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Eventually(func() error {
				updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node1", metav1.GetOptions{})
				if err != nil {
					return err
				}

				updatedNode1JoinSubnetAnnotation, ok := updatedNode.Annotations["k8s.ovn.org/zone-join-subnets"]
				if !ok {
					return fmt.Errorf("expected node annotation for node node1 to have zone join subnets")
				}

				if updatedNode1JoinSubnetAnnotation != globalZoneJoinSubnetForNode1 {
					return fmt.Errorf("join subnet annotation for node node1 should have the global zone join subnet")
				}

				joinSubnets, err := util.ParseZoneJoinSubnetsAnnotation(updatedNode)
				if err != nil {
					return fmt.Errorf("error parsing zone join subnet annotation for the node node1")
				}

				if len(joinSubnets) < 1 {
					return fmt.Errorf("zone join subnet annotation for node1 is empty")
				}
				return nil
			}).ShouldNot(gomega.HaveOccurred())
			return nil
		}

		err := app.Run([]string{
			app.Name,
			"-cluster-subnets=" + clusterCIDR,
			"-init-cluster-manager",
			"-zone-join-switch-subnets=" + joinSubnetCIDR,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Cluster Manager Node subnet allocations - hybrid and linux nodes", func() {

		app.Action = func(ctx *cli.Context) error {
			nodes := []v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node3",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "winnode",
						Labels: map[string]string{v1.LabelOSStable: "windows"},
					},
				}}
			kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
				Items: nodes,
			})
			egressFirewallFakeClient := &egressfirewallfake.Clientset{}
			egressIPFakeClient := &egressipfake.Clientset{}
			egressQoSFakeClient := &egressqosfake.Clientset{}
			fakeClient := &util.OVNClientset{
				KubeClient:           kubeFakeClient,
				EgressIPClient:       egressIPFakeClient,
				EgressFirewallClient: egressFirewallFakeClient,
				EgressQoSClient:      egressQoSFakeClient,
			}

			_, err := config.InitConfig(ctx, nil, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			config.Kubernetes.HostNetworkNamespace = ""

			f, err = factory.NewClusterManagerWatchFactory(fakeClient)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = f.Start()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			clusterManager := NewClusterManager(fakeClient, f, stopChan,
				record.NewFakeRecorder(0))
			gomega.Expect(clusterManager).NotTo(gomega.BeNil())
			for _, clusterEntry := range config.HybridOverlay.ClusterSubnets {
				clusterManager.AddHybridOverlaySubnetNetworkRange(clusterEntry.CIDR, clusterEntry.HostSubnetLength)
			}

			for _, clusterEntry := range config.Default.ClusterSubnets {
				clusterManager.AddClusterSubnetNetworkRange(clusterEntry.CIDR, clusterEntry.HostSubnetLength)
			}
			gomega.Expect(clusterManager.WatchNodes()).To(gomega.Succeed())

			// Check that cluster manager has set the subnet annotation for each node.
			for _, n := range nodes {
				if n.Name == "winnode" {
					continue
				}

				gomega.Eventually(func() ([]*net.IPNet, error) {
					updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
					if err != nil {
						return nil, err
					}

					return util.ParseNodeHostSubnetAnnotation(updatedNode)
				}, 2).Should(gomega.HaveLen(1))
			}

			// Windows node should be allocated a subnet
			gomega.Eventually(func() (map[string]string, error) {
				updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "winnode", metav1.GetOptions{})
				if err != nil {
					return nil, err
				}
				return updatedNode.Annotations, nil
			}, 2).Should(gomega.HaveKey(hotypes.HybridOverlayNodeSubnet))

			gomega.Eventually(func() error {
				updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "winnode", metav1.GetOptions{})
				if err != nil {
					return err
				}
				_, err = util.ParseNodeHostSubnetAnnotation(updatedNode)
				return err
			}, 2).Should(gomega.MatchError(fmt.Sprintf("node %q has no \"k8s.ovn.org/node-subnets\" annotation", "winnode")))

			return nil
		}

		err := app.Run([]string{
			app.Name,
			"--no-hostsubnet-nodes=kubernetes.io/os=windows",
			"-cluster-subnets=" + clusterCIDR,
			"-gateway-mode=shared",
			"-enable-hybrid-overlay",
			"-hybrid-overlay-cluster-subnets=" + hybridOverlayClusterCIDR,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
