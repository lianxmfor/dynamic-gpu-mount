package nsenter

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/NVIDIA/gpu-monitoring-tools/bindings/go/nvml"
	v1 "k8s.io/api/core/v1"
)

const nsenter = "nsenter"

// MountGpu 利用nsenter在docker container中添加/nvidia0-7文件
// containerHostPid: container init进程在宿主机上的进程id
// nvidiaDevice: gpu device file. /dev/nvidia0,/dev/nvidia1,/dev/nvidia2 etc...
func MountGpu(ctx context.Context, containerHostPid string, nvidiaDevice string) (string, error) {
	if _, err := exec.LookPath(nsenter); err != nil {
		return "", err
	}

	nsenterArgs := []string{"--target", containerHostPid, "--mount", "--uts", "--ipc", "--net", "--pid"}
	nvidiaDeviceId := ""
	fmt.Sscanf(nvidiaDevice, "/dev/nvidia%s", &nvidiaDeviceId)
	mknodCmd := []string{"mknod", "-m", "666", fmt.Sprintf("/dev/nvidia%s", nvidiaDeviceId), "c", "195", nvidiaDeviceId}
	out, err := exec.CommandContext(ctx, nsenter, append(nsenterArgs, mknodCmd...)...).CombinedOutput()
	return string(out), err
}

// UnMountGpu 利用nsenter将docker container中的/nvidia0-7文件删除，从而达到卸载gpu的目的
// containerHostPid: container init进程在宿主机上的进程id
func UnMountGpu(ctx context.Context, containerHostPid string) error {
	if _, err := exec.LookPath(nsenter); err != nil {
		return err
	}

	nsenterArgs := []string{"--target", containerHostPid, "--mount", "--uts", "--ipc", "--net", "--pid"}
	count, err := nvml.GetDeviceCount()
	if err != nil {
		return err
	}
	for i := uint(0); i < count; i++ {
		cmd := []string{"rm", fmt.Sprintf("/dev/nvidia%d", i)}
		exec.CommandContext(ctx, nsenter, append(nsenterArgs, cmd...)...).CombinedOutput()
	}
	return nil
}

// GetContainerHostPid 给定pod，获取第一个container init进程在宿主机上的进程id
func GetContainerHostPid(ctx context.Context, pod v1.Pod) (string, error) {
	containerId := strings.TrimLeft(pod.Status.ContainerStatuses[0].ContainerID, "docker://")
	out, err := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Pid}}", containerId).CombinedOutput()
	return strings.Trim(string(out), "\n"), err
}
