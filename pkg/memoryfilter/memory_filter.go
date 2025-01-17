package memoryfilter

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type MemoryFilter struct {
	handle framework.Handle
}

var _ framework.FilterPlugin = &MemoryFilter{}

const MemoryFilterName = "MemoryFilter"

func (m *MemoryFilter) Name() string {
	return MemoryFilterName
}

func New(ctx context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return &MemoryFilter{
		handle: handle,
	}, nil
}

func (m *MemoryFilter) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	requests := getMemoryRequests(pod)
	allocatable := nodeInfo.Node().Status.Allocatable
	available := allocatable[v1.ResourceMemory]

	if available.Cmp(requests) < 0 {
		return framework.NewStatus(framework.Unschedulable, "InsufficientMemory")
	}

	return framework.NewStatus(framework.Success, "")
}

func getMemoryRequests(pod *v1.Pod) resource.Quantity {
	var requests resource.Quantity
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			if memoryReq, ok := container.Resources.Requests[v1.ResourceMemory]; ok {
				requests.Add(memoryReq)
			}
		}
	}
	return requests
}
