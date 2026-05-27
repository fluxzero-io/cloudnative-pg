/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package persistentvolumeclaim

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PVCs used by instance", func() {
	clusterName := "cluster-pvc-instance"
	instanceName := clusterName + "-1"

	cluster := &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterName,
		},
		Spec: apiv1.ClusterSpec{
			WalStorage: &apiv1.StorageConfiguration{},
		},
	}

	It("true if the pvc belongs to the instance name", func() {
		res := BelongToInstance(cluster, instanceName, instanceName)
		Expect(res).To(BeTrue())

		res = BelongToInstance(cluster, instanceName, instanceName+"-wal")
		Expect(res).To(BeTrue())
	})

	It("fails when trying to get a pvc that doesn't belong to the instance", func() {
		res := BelongToInstance(cluster, instanceName, instanceName+"-nil")
		Expect(res).To(BeFalse())
	})
})

var _ = Describe("offline resize detection", func() {
	const (
		clusterName    = "cluster-offline-resize"
		instanceName   = clusterName + "-1"
		tablespaceName = "archive"
	)

	makeOfflineResizePVC := func(name string, labels map[string]string) corev1.PersistentVolumeClaim {
		return corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: labels,
				Annotations: map[string]string{
					utils.PVCStatusAnnotationName: StatusReady,
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("2Gi"),
					},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimBound,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		}
	}

	makeCluster := func() *apiv1.Cluster {
		return &apiv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName},
			Spec: apiv1.ClusterSpec{
				StorageConfiguration: apiv1.StorageConfiguration{
					ResizeStrategy: apiv1.StorageResizeStrategyOnline,
				},
				WalStorage: &apiv1.StorageConfiguration{
					ResizeStrategy: apiv1.StorageResizeStrategyOffline,
				},
				Tablespaces: []apiv1.TablespaceConfiguration{
					{
						Name: tablespaceName,
						Storage: apiv1.StorageConfiguration{
							ResizeStrategy: apiv1.StorageResizeStrategyOffline,
						},
					},
				},
			},
		}
	}

	It("uses allocated resource status to detect controller-side resize", func() {
		calculator := NewPgWalCalculator()
		pvc := makeOfflineResizePVC(calculator.GetName(instanceName), calculator.GetLabels(instanceName))
		pvc.Status.Capacity[corev1.ResourceStorage] = resource.MustParse("2Gi")
		pvc.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{
			corev1.ResourceStorage: corev1.PersistentVolumeClaimControllerResizeInProgress,
		}

		Expect(getResizeState(pvc)).To(Equal(resizeStateControllerPending))
		Expect(IsOfflineResizePending(makeCluster(), pvc)).To(BeTrue())
	})

	It("allows pod creation once the controller resize has reached node resize", func() {
		calculator := NewPgWalCalculator()
		pvc := makeOfflineResizePVC(calculator.GetName(instanceName), calculator.GetLabels(instanceName))
		pvc.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{
			corev1.ResourceStorage: corev1.PersistentVolumeClaimNodeResizePending,
		}

		Expect(getResizeState(pvc)).To(Equal(resizeStateNodePending))
		Expect(IsOfflineResizePending(makeCluster(), pvc)).To(BeFalse())
	})

	It("does not classify failed controller resize as offline pending", func() {
		calculator := NewPgWalCalculator()
		pvc := makeOfflineResizePVC(calculator.GetName(instanceName), calculator.GetLabels(instanceName))
		pvc.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{
			corev1.ResourceStorage: corev1.PersistentVolumeClaimControllerResizeInfeasible,
		}

		Expect(getResizeState(pvc)).To(Equal(resizeStateFailed))
		Expect(IsOfflineResizePending(makeCluster(), pvc)).To(BeFalse())
	})

	It("uses the WAL storage resize strategy for WAL PVCs", func() {
		calculator := NewPgWalCalculator()
		pvc := makeOfflineResizePVC(calculator.GetName(instanceName), calculator.GetLabels(instanceName))

		Expect(IsOfflineResizePending(makeCluster(), pvc)).To(BeTrue())
	})

	It("uses the tablespace storage resize strategy for tablespace PVCs", func() {
		calculator := NewPgTablespaceCalculator(tablespaceName)
		pvc := makeOfflineResizePVC(calculator.GetName(instanceName), calculator.GetLabels(instanceName))

		Expect(IsOfflineResizePending(makeCluster(), pvc)).To(BeTrue())
	})

	It("checks all PVCs attached to an instance pod", func() {
		walCalculator := NewPgWalCalculator()
		tablespaceCalculator := NewPgTablespaceCalculator(tablespaceName)
		walPVCName := walCalculator.GetName(instanceName)
		tablespacePVCName := tablespaceCalculator.GetName(instanceName)
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: instanceName,
							},
						},
					},
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: walPVCName,
							},
						},
					},
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: tablespacePVCName,
							},
						},
					},
				},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			makeOfflineResizePVC(walPVCName, walCalculator.GetLabels(instanceName)),
			makeOfflineResizePVC(tablespacePVCName, tablespaceCalculator.GetLabels(instanceName)),
		}

		Expect(IsOfflineResizePendingForPod(makeCluster(), pod, pvcs)).To(BeTrue())
	})
})

var _ = Describe("instance with tablespace test", func() {
	clusterName := "cluster-tbs-pvc-instance"
	instanceName := clusterName + "-1"

	cluster := &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterName,
		},
		Spec: apiv1.ClusterSpec{
			StorageConfiguration: apiv1.StorageConfiguration{},
			WalStorage:           &apiv1.StorageConfiguration{},
			Tablespaces: []apiv1.TablespaceConfiguration{
				{
					Name: "tbs1",
					Storage: apiv1.StorageConfiguration{
						Size: "1Gi",
					},
				},
				{
					Name: "tbs2",
					Storage: apiv1.StorageConfiguration{
						Size: "1Gi",
					},
				},
				{
					Name: "tbs3",
					Storage: apiv1.StorageConfiguration{
						Size: "1Gi",
					},
				},
			},
		},
	}

	It("Get all the expected pvc out", func() {
		expectedPVCs := getExpectedPVCsFromCluster(cluster, instanceName)
		Expect(expectedPVCs).Should(HaveLen(5))
		for _, pvc := range expectedPVCs {
			Expect(pvc.name).Should(Equal(pvc.calculator.GetName(instanceName)))
		}
	})
})
