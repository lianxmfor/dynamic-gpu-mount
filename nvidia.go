package main

import (
	"log"
	"strings"

	"github.com/NVIDIA/gpu-monitoring-tools/bindings/go/nvml"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type Device struct {
	pluginapi.Device
	Path string
}

type ResourceManager interface {
	Devices() []*Device
	CheckHealth(stop <-chan struct{}, devices []*Device, unhealthy chan<- *Device)
}

type GpuDeviceManager struct {
}

func (g *GpuDeviceManager) Devices() []*Device {
	n, err := nvml.GetDeviceCount()
	check(err)

	var devs []*Device
	for i := uint(0); i < n; i++ {
		dev, err := nvml.NewDeviceLite(i)
		check(err)

		devs = append(devs, dev)
	}
	return devs
}

func (g *GpuDeviceManager) CheckHealth(stop <-chan struct{}, devices []*Device, unhealthy chan<- *Device) {
	eventSet := nvml.NewEventSet()
	defer nvml.DeleteEventSet(eventSet)

	for _, dev := range devices {
		gpu, _, _, err := nvml.ParseMigDeviceUUID(dev.ID)
		if err != nil {
			gpu = dev.ID
		}

		err = nvml.RegisterEventForDevice(eventSet, nvml.XidCriticalError, gpu)
		if err != nil && strings.HasSuffix(err.Error(), "Not Supported") {
			log.Printf("Warning: %s is too old to support healthchecking: %s. Marking it unhealthy.", dev.ID, err)
			unhealthy <- dev
			continue
		}
		check(err)
	}

	for {
		select {
		case <-stop:
			return
		default:
		}

		e, err := nvml.WaitForEvent(eventSet, 5000)
		if err != nil && e.Etype != nvml.XidCriticalError {
			continue
		}

		if e.Edata == 31 || e.Edata == 43 || e.Edata == 45 {
			continue
		}

		if e.UUID == nil || len(*e.UUID) == 0 {
			// All devices are unhealthy
			log.Printf("XidCriticalError: Xid=%d, All devices will go unhealthy.", e.Edata)
			for _, dev := range devices {
				unhealthy <- dev
			}
			continue
		}

		for _, dev := range devices {
			gpu, gi, ci, err := nvml.ParseMigDeviceUUID(dev.ID)
			if err != nil {
				gpu = dev.ID
				gi = 0xFFFFFFFF
				ci = 0xFFFFFFFF
			}

			if gpu == *e.UUID && gi == *e.GpuInstanceId && ci == *e.ComputeInstanceId {
				log.Printf("XidCriticalError: Xid=%d on Device=%s, the device will go unhealthy.", e.Edata, dev.ID)
				unhealthy <- dev
			}
		}

	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
