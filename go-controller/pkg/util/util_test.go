package util

import (
	"github.com/urfave/cli"
	kapi "k8s.io/api/core/v1"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Util tests", func() {
	var app *cli.App

	BeforeEach(func() {
		// Restore global default values before each testcase
		config.RestoreDefaultConfig()

		app = cli.NewApp()
		app.Name = "test"
		app.Flags = config.Flags
	})

	It("test validateOVNConfigEndpoint()", func() {

		type testcase struct {
			name           string
			subsets        []kapi.EndpointSubset
			expectedResult bool
		}

		testcases := []testcase{
			{
				name: "valid endpoint",
				subsets: []kapi.EndpointSubset{
					{
						Addresses: []kapi.EndpointAddress{
							{IP: "10.1.2.3"},
						},
						Ports: []kapi.EndpointPort{
							{
								Name: "north",
								Port: 1234,
							},
							{
								Name: "south",
								Port: 4321,
							},
						},
					},
				},
				expectedResult: true,
			},
			{
				name: "invalid endpoint two few ports",
				subsets: []kapi.EndpointSubset{
					{
						Addresses: []kapi.EndpointAddress{
							{IP: "10.1.2.3"},
						},
						Ports: []kapi.EndpointPort{
							{
								Name: "north",
								Port: 1234,
							},
						},
					},
				},
				expectedResult: false,
			},
			{
				name: "invalid endpoint too many ports",
				subsets: []kapi.EndpointSubset{
					{
						Addresses: []kapi.EndpointAddress{
							{IP: "10.1.2.3"},
						},
						Ports: []kapi.EndpointPort{
							{
								Name: "north",
								Port: 1234,
							},
							{
								Name: "south",
								Port: 4321,
							},
							{
								Name: "east",
								Port: 7654,
							},
						},
					},
				},
				expectedResult: false,
			},
			{
				name:           "invalid endpoint no subsets",
				subsets:        []kapi.EndpointSubset{},
				expectedResult: false,
			},
		}

		for _, tc := range testcases {
			test := kapi.Endpoints{
				Subsets: tc.subsets,
			}
			Expect(isValidOVNConfigEndpoint(&test)).To(Equal(tc.expectedResult), " test case \"%s\" returned %t instead of %t", tc.name, !tc.expectedResult, tc.expectedResult)
		}
	})

	It("test ExtractDbRemotesFromEndpoint()", func() {
		//valid endpoint
		subsets := []kapi.EndpointSubset{
			{
				Addresses: []kapi.EndpointAddress{
					{IP: "10.1.2.3"},
				},
				Ports: []kapi.EndpointPort{
					{
						Name: "north",
						Port: 1234,
					},
					{
						Name: "south",
						Port: 4321,
					},
				},
			},
		}
		test := kapi.Endpoints{
			Subsets: subsets,
		}
		masterIPList, sbDBPort, nbDBPort, err := ExtractDbRemotesFromEndpoint(&test)
		Expect(err).NotTo(HaveOccurred())
		Expect(nbDBPort).To(Equal(int32(1234)), " test case returned %t instead of 1234", nbDBPort)
		Expect(sbDBPort).To(Equal(int32(4321)), " test case returned %t instead of 4321", sbDBPort)
		Expect(masterIPList).To(Equal("10.1.2.3"), " test case returned %t instead of \"10.1.2.3\"", masterIPList)

		//invalid endpoint two few ports
		subsets = []kapi.EndpointSubset{
			{
				Addresses: []kapi.EndpointAddress{
					{IP: "10.1.2.3"},
				},
				Ports: []kapi.EndpointPort{
					{
						Name: "north",
						Port: 1234,
					},
				},
			},
		}
		test = kapi.Endpoints{
			Subsets: subsets,
		}
		_, _, _, err = ExtractDbRemotesFromEndpoint(&test)
		Expect(err).NotTo(BeNil())

		//invalid endpoint too many ports
		subsets = []kapi.EndpointSubset{
			{
				Addresses: []kapi.EndpointAddress{
					{IP: "10.1.2.3"},
				},
				Ports: []kapi.EndpointPort{
					{
						Name: "north",
						Port: 1234,
					},
					{
						Name: "south",
						Port: 4321,
					},
					{
						Name: "east",
						Port: 7654,
					},
				},
			},
		}
		test = kapi.Endpoints{
			Subsets: subsets,
		}
		_, _, _, err = ExtractDbRemotesFromEndpoint(&test)
		Expect(err).NotTo(BeNil())

		//invalid endpoint, no IPs
		subsets = []kapi.EndpointSubset{
			{
				Addresses: []kapi.EndpointAddress{},
				Ports: []kapi.EndpointPort{
					{
						Name: "north",
						Port: 1234,
					},
					{
						Name: "south",
						Port: 4321,
					},
				},
			},
		}

		test = kapi.Endpoints{
			Subsets: subsets,
		}
		_, _, _, err = ExtractDbRemotesFromEndpoint(&test)
		Expect(err).NotTo(BeNil())
	})
})
