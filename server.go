package main

import (
	"context"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type GpuDevicePlugin struct {
	ResourceManager
	resourceName string
	socket       string

	server       *grpc.Server
	cacheDevices []*Device
	health       chan *Device
	stop         chan struct{}
}

func NewGpuDevicePlugin(resourceName string, socketName string) *GpuDevicePlugin {
	return &GpuDevicePlugin{
		resourceName: resourceName,
		socket:       filepath.Join(pluginapi.DevicePluginPath, socketName),

		server: nil,
		health: nil,
		stop:   nil,
	}
}

func (g *GpuDevicePlugin) initalize() {
	g.cacheDevices = g.Devices()
	g.server = grpc.NewServer([]grpc.ServerOption{}...)
	g.health = make(chan *Device)
	g.stop = make(chan struct{})
}

func (g *GpuDevicePlugin) cleanup() {
	close(g.stop)
	g.cacheDevices = nil
	g.server = nil
	g.health = nil
	g.stop = nil
}

func (g *GpuDevicePlugin) dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	c, err := grpc.Dial(unixSocketPath, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(timeout),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))

	return c, err
}

// Serve starts the gRPC server of the device plugin
func (g *GpuDevicePlugin) Serve() error {
	os.Remove(g.socket)
	sock, err := net.Listen("unix", g.socket)
	if err != nil {
		return nil
	}

	pluginapi.RegisterDevicePluginServer(g.server, g)

	go func() {
		restartCount := 0
		for {
			log.Printf("Start GRPC server for '%s'", g.resourceName)
			err := g.server.Serve(sock)
			if err == nil {
				break
			}

			log.Printf("GRPC server for '%s' crashed with error: %v", g.resourceName, err)

			if restartCount > 5 {
				// quit
				log.Fatalf("GRPC server for '%s' has repeatedly crashed recently. Quiting", g.resourceName)
			}
			restartCount++
		}
	}()

	// Wait for server to start by launching a blocking connexion
	conn, err := g.dial(g.socket, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

// Register registers the device plugin for the given resourceName with kubelet.
func (g *GpuDevicePlugin) Register() error {
	conn, err := g.dial(pluginapi.KubeletSocket, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	reqt := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(g.socket),
		ResourceName: g.resourceName,
	}

	_, err = client.Register(context.Background(), reqt)
	return err
}

func (g *GpuDevicePlugin) Stop() error {
	if g == nil || g.server == nil {
		return nil
	}
	log.Printf("Stopping to server '%s' on %s", g.resourceName, g.socket)
	g.server.Stop()
	if err := os.Remove(g.socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	g.cleanup()
	return nil
}

func (g *GpuDevicePlugin) Start() error {
	g.initalize()

	err := g.Serve()
	if err != nil {
		log.Printf("Cound not start device plugin for '%s': %s", g.resourceName, err)
		g.cleanup()
		return err
	}

	log.Printf("starting to serve '%s' on %s", g.resourceName, g.socket)

	err = g.Register()
	if err != nil {
		log.Panicf("Could not register device plugin: %s", err)
		g.Stop()
		return err
	}
	log.Panicf("Register device plugin for '%s' with kubelet", g.resourceName)

	go g.CheckHealth(g.stop, g.cacheDevices, g.health)

	return nil
}

func (g *GpuDevicePlugin) apiDevices() []*pluginapi.Device {
	var pdevs []*pluginapi.Device
	for _, dev := range g.cacheDevices {
		pdevs = append(pdevs, &dev.Device)
	}
	return pdevs
}

// GetDevicePluginOptions returns options to be communicated with Device
// Manager
func (g *GpuDevicePlugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{
		PreStartRequired:                false,
		GetPreferredAllocationAvailable: false,
	}, nil
}

// ListAndWatch returns a stream of List of Devices
// Whenever a Device state change or a Device disappears, ListAndWatch
// returns the new list
func (g *GpuDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	log.Println("exposing devices: ", g.apiDevices())
	s.Send(&pluginapi.ListAndWatchResponse{Devices: g.apiDevices()})

	for {
		select {
		case <-g.stop:
			return nil
		case d := <-g.health:
			d.Health = pluginapi.Unhealthy
			log.Printf("'%s' device marked unhealthy: %s", g.resourceName, d.ID)
			s.Send(&pluginapi.ListAndWatchResponse{Devices: g.apiDevices()})
		}
	}
}

// GetPreferredAllocation returns a preferred set of devices to allocate
// from a list of available ones. The resulting preferred allocation is not
// guaranteed to be the allocation ultimately performed by the
// devicemanager. It is only designed to help the devicemanager make a more
// informed allocation decision when possible.
func (g *GpuDevicePlugin) GetPreferredAllocation(ctx context.Context, r *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return nil, nil
}

// Allocate is called during container creation so that the Device
// Plugin can run device specific operations and instruct Kubelet
// of the steps to make the Device available in the container
func (g *GpuDevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	return nil, nil
}

// PreStartContainer is called, if indicated by Device Plugin during registeration phase,
// before each container start. Device plugin can run device specific operations
// such as resetting the device before making devices available to the container
func (g *GpuDevicePlugin) PreStartContainer(ctx context.Context, reqs *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return nil, nil
}
