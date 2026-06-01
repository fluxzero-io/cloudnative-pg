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

package controller

import (
	"context"
	"errors"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	k8client "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/internal/configuration"
	rolloutManager "github.com/cloudnative-pg/cloudnative-pg/internal/controller/rollout"
	schemeBuilder "github.com/cloudnative-pg/cloudnative-pg/internal/scheme"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/postgres"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/reconciler/persistentvolumeclaim"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/specs"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Offline PVC resize controller", func() {
	const namespace = "offline-resize-test"

	var (
		reconciler *ClusterReconciler
		rm         *rolloutManager.Manager
		k8sClient  k8client.Client
	)

	BeforeEach(func() {
		scheme := schemeBuilder.BuildWithAllKnownScheme()
		k8sClient = fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&apiv1.Cluster{}).
			Build()

		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}
		Expect(k8sClient.Create(context.Background(), ns)).To(Succeed())

		rm = rolloutManager.New(time.Hour, 0)
		reconciler = &ClusterReconciler{
			Client:         k8sClient,
			Scheme:         scheme,
			Recorder:       record.NewFakeRecorder(120),
			rolloutManager: rm,
		}

		configuration.Current = configuration.NewConfiguration()
	})

	buildResizePod := func(cluster *apiv1.Cluster, serial int) *corev1.Pod {
		name := specs.GetInstanceName(cluster.Name, serial)
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: cluster.Namespace,
				Annotations: map[string]string{
					utils.ClusterSerialAnnotationName: strconv.Itoa(serial),
				},
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: name,
							},
						},
					},
				},
			},
		}
	}

	buildResizePVC := func(cluster *apiv1.Cluster, serial int, capacity string) corev1.PersistentVolumeClaim {
		name := specs.GetInstanceName(cluster.Name, serial)
		return corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: cluster.Namespace,
				Annotations: map[string]string{
					utils.PVCStatusAnnotationName: persistentvolumeclaim.StatusReady,
				},
				Labels: persistentvolumeclaim.NewPgDataCalculator().GetLabels(name),
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
					corev1.ResourceStorage: resource.MustParse(capacity),
				},
			},
		}
	}

	createOfflineResizeClusterWithStrategy := func(
		instances int,
		strategy apiv1.PrimaryUpdateStrategy,
	) *apiv1.Cluster {
		cluster := &apiv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: namespace,
			},
			Spec: apiv1.ClusterSpec{
				Instances:             instances,
				PrimaryUpdateStrategy: strategy,
				PrimaryUpdateMethod:   apiv1.PrimaryUpdateMethodRestart,
				StorageConfiguration: apiv1.StorageConfiguration{
					Size:           "2Gi",
					ResizeStrategy: apiv1.StorageResizeStrategyOffline,
				},
			},
		}
		cluster.SetDefaults()
		cluster.Status.CurrentPrimary = "test-cluster-1"
		cluster.Status.TargetPrimary = "test-cluster-1"
		cluster.Status.Instances = instances
		cluster.Status.ReadyInstances = instances
		cluster.Status.Image = "postgres:16.1"
		Expect(k8sClient.Create(context.Background(), cluster)).To(Succeed())
		Expect(k8sClient.Status().Update(context.Background(), cluster)).To(Succeed())
		return cluster
	}

	createOfflineResizeCluster := func(instances int) *apiv1.Cluster {
		return createOfflineResizeClusterWithStrategy(instances, "")
	}

	buildOfflineResizeResources := func(
		pvcs []corev1.PersistentVolumeClaim,
		pods ...*corev1.Pod,
	) *managedResources {
		podItems := make([]corev1.Pod, 0, len(pods))
		for _, pod := range pods {
			podItems = append(podItems, *pod)
		}
		return &managedResources{
			instances: corev1.PodList{Items: podItems},
			pvcs:      corev1.PersistentVolumeClaimList{Items: pvcs},
		}
	}

	It("deletes a resized replica pod before touching the primary", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())

		podList := &postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
				{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: true},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			buildResizePVC(cluster, 1, "1Gi"),
			buildResizePVC(cluster, 2, "1Gi"),
		}

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(pvcs, primaryPod, replicaPod),
			*podList,
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeFalse())

		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).ToNot(Succeed())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(primaryPod), &corev1.Pod{})).To(Succeed())
	})

	It("does not delete a fenced replica during offline resize", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		cluster.Annotations = map[string]string{
			utils.FencedInstanceAnnotation: `["test-cluster-2"]`,
		}
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())

		podList := &postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
				{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: true},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			buildResizePVC(cluster, 1, "2Gi"),
			buildResizePVC(cluster, 2, "1Gi"),
		}

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(pvcs, primaryPod, replicaPod),
			*podList,
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeTrue())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).To(Succeed())
	})

	It("does not act while hibernation is enabled", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		cluster.Annotations = map[string]string{
			utils.HibernationAnnotationName: string(utils.HibernationAnnotationValueOn),
		}
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(
				[]corev1.PersistentVolumeClaim{
					buildResizePVC(cluster, 1, "2Gi"),
					buildResizePVC(cluster, 2, "1Gi"),
				},
				primaryPod,
				replicaPod,
			),
			postgres.PostgresqlStatusList{
				Items: []postgres.PostgresqlStatus{
					{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
					{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: true},
				},
			},
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeTrue())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).To(Succeed())
	})

	It("does not act during an in-flight switchover", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		cluster.Status.TargetPrimary = "test-cluster-2"
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(
				[]corev1.PersistentVolumeClaim{
					buildResizePVC(cluster, 1, "2Gi"),
					buildResizePVC(cluster, 2, "1Gi"),
				},
				primaryPod,
				replicaPod,
			),
			postgres.PostgresqlStatusList{
				Items: []postgres.PostgresqlStatus{
					{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
					{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: true},
				},
			},
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeTrue())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).To(Succeed())
	})

	It("does not act while the cluster is unrecoverable", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		cluster.Status.Phase = apiv1.PhaseUnrecoverable
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(
				[]corev1.PersistentVolumeClaim{
					buildResizePVC(cluster, 1, "2Gi"),
					buildResizePVC(cluster, 2, "1Gi"),
				},
				primaryPod,
				replicaPod,
			),
			postgres.PostgresqlStatusList{
				Items: []postgres.PostgresqlStatus{
					{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
					{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: true},
				},
			},
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeTrue())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).To(Succeed())
	})

	It("delays pod deletion when the rollout manager is busy", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())
		Expect(rm.CoordinateRollout(k8client.ObjectKey{Namespace: namespace, Name: "other"}, "other").RolloutAllowed).
			To(BeTrue())

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(
				[]corev1.PersistentVolumeClaim{
					buildResizePVC(cluster, 1, "2Gi"),
					buildResizePVC(cluster, 2, "1Gi"),
				},
				primaryPod,
				replicaPod,
			),
			postgres.PostgresqlStatusList{
				Items: []postgres.PostgresqlStatus{
					{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
					{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: true},
				},
			},
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(15 * time.Second))
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).To(Succeed())
	})

	It("switches primary to a caught-up streaming replica before resizing the former primary", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		podList := &postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash", CurrentLsn: "0/2000000"},
				{
					Pod:                 replicaPod,
					IsPodReady:          false,
					ExecutableHash:      "hash",
					IsWalReceiverActive: true,
					ReceivedLsn:         "0/2000000",
					ReplayLsn:           "0/2000000",
				},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			buildResizePVC(cluster, 1, "1Gi"),
			buildResizePVC(cluster, 2, "2Gi"),
		}

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(pvcs, primaryPod, replicaPod),
			*podList,
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeFalse())

		var updatedCluster apiv1.Cluster
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(cluster), &updatedCluster)).To(Succeed())
		Expect(updatedCluster.Status.TargetPrimary).To(Equal(replicaPod.Name))
	})

	It("switches primary to a later healthy resized replica when the first replica is unavailable", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(3)
		primaryPod := buildResizePod(cluster, 1)
		unavailableReplicaPod := buildResizePod(cluster, 2)
		healthyReplicaPod := buildResizePod(cluster, 3)
		podList := &postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
				{
					Pod:                 unavailableReplicaPod,
					IsPodReady:          false,
					MightBeUnavailable:  true,
					ExecutableHash:      "hash",
					IsWalReceiverActive: true,
					ReceivedLsn:         "0/2000000",
					ReplayLsn:           "0/2000000",
				},
				{
					Pod:                 healthyReplicaPod,
					IsPodReady:          true,
					ExecutableHash:      "hash",
					IsWalReceiverActive: true,
					ReceivedLsn:         "0/2000000",
					ReplayLsn:           "0/2000000",
				},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			buildResizePVC(cluster, 1, "1Gi"),
			buildResizePVC(cluster, 2, "2Gi"),
			buildResizePVC(cluster, 3, "2Gi"),
		}

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(pvcs, primaryPod, unavailableReplicaPod, healthyReplicaPod),
			*podList,
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeFalse())

		var updatedCluster apiv1.Cluster
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(cluster), &updatedCluster)).To(Succeed())
		Expect(updatedCluster.Status.TargetPrimary).To(Equal(healthyReplicaPod.Name))
	})

	It("waits for a manual switchover before resizing a supervised primary", func(ctx SpecContext) {
		cluster := createOfflineResizeClusterWithStrategy(2, apiv1.PrimaryUpdateStrategySupervised)
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())
		podList := &postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
				{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: true},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			buildResizePVC(cluster, 1, "1Gi"),
			buildResizePVC(cluster, 2, "2Gi"),
		}

		_, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(pvcs, primaryPod, replicaPod),
			*podList,
		)
		Expect(err).To(MatchError(ErrNextLoop))

		var updatedCluster apiv1.Cluster
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(cluster), &updatedCluster)).To(Succeed())
		Expect(updatedCluster.Status.Phase).To(Equal(apiv1.PhaseWaitingForUser))
		Expect(updatedCluster.Status.TargetPrimary).To(Equal(primaryPod.Name))
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(primaryPod), &corev1.Pod{})).To(Succeed())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).To(Succeed())

		secondCluster := k8client.ObjectKey{Namespace: namespace, Name: "other-cluster"}
		result := rm.CoordinateRollout(secondCluster, "other-pod")
		Expect(result.RolloutAllowed).To(BeTrue(),
			"supervised offline resize should not consume the rollout slot")
	})

	It("does not stop the primary without a healthy resized replica", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())
		podList := &postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
				{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: false},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			buildResizePVC(cluster, 1, "1Gi"),
			buildResizePVC(cluster, 2, "2Gi"),
		}

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(pvcs, primaryPod, replicaPod),
			*podList,
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(5 * time.Second))
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(primaryPod), &corev1.Pod{})).To(Succeed())
	})

	It("does not stop the primary when the resized replica is too far behind", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())
		podList := &postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash", CurrentLsn: "0/3000000"},
				{
					Pod:                 replicaPod,
					IsPodReady:          true,
					ExecutableHash:      "hash",
					IsWalReceiverActive: true,
					ReceivedLsn:         "0/3000000",
					ReplayLsn:           "0/1000000",
				},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			buildResizePVC(cluster, 1, "1Gi"),
			buildResizePVC(cluster, 2, "2Gi"),
		}

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(pvcs, primaryPod, replicaPod),
			*podList,
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(5 * time.Second))
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(primaryPod), &corev1.Pod{})).To(Succeed())
	})

	It("deletes the only instance during single-instance offline resize", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(1)
		primaryPod := buildResizePod(cluster, 1)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		podList := &postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
			},
		}
		pvcs := []corev1.PersistentVolumeClaim{
			buildResizePVC(cluster, 1, "1Gi"),
		}

		result, err := reconciler.reconcileOfflinePVCResize(
			ctx,
			cluster,
			buildOfflineResizeResources(pvcs, primaryPod),
			*podList,
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeFalse())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(primaryPod), &corev1.Pod{})).ToNot(Succeed())
	})

	It("deletes a non-ready replica waiting for offline resize", func(ctx SpecContext) {
		cluster := createOfflineResizeCluster(2)
		primaryPod := buildResizePod(cluster, 1)
		replicaPod := buildResizePod(cluster, 2)
		Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
		Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())

		resources := &managedResources{
			instances: corev1.PodList{Items: []corev1.Pod{*primaryPod, *replicaPod}},
			pvcs: corev1.PersistentVolumeClaimList{Items: []corev1.PersistentVolumeClaim{
				buildResizePVC(cluster, 1, "2Gi"),
				buildResizePVC(cluster, 2, "1Gi"),
			}},
		}
		podList := postgres.PostgresqlStatusList{
			Items: []postgres.PostgresqlStatus{
				{Pod: primaryPod, IsPrimary: true, IsPodReady: true, ExecutableHash: "hash"},
				{Pod: replicaPod, IsPodReady: false, Error: errors.New("startup failed")},
			},
		}

		result, err := reconciler.reconcileOfflinePVCResize(ctx, cluster, resources, podList)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeFalse())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).ToNot(Succeed())
		Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(primaryPod), &corev1.Pod{})).To(Succeed())
	})

	It("does not delete a non-ready primary while a multi-instance offline resize still needs switchover",
		func(ctx SpecContext) {
			cluster := createOfflineResizeCluster(2)
			primaryPod := buildResizePod(cluster, 1)
			replicaPod := buildResizePod(cluster, 2)
			Expect(k8sClient.Create(ctx, primaryPod)).To(Succeed())
			Expect(k8sClient.Create(ctx, replicaPod)).To(Succeed())

			resources := &managedResources{
				instances: corev1.PodList{Items: []corev1.Pod{*primaryPod, *replicaPod}},
				pvcs: corev1.PersistentVolumeClaimList{Items: []corev1.PersistentVolumeClaim{
					buildResizePVC(cluster, 1, "1Gi"),
					buildResizePVC(cluster, 2, "2Gi"),
				}},
			}
			podList := postgres.PostgresqlStatusList{
				Items: []postgres.PostgresqlStatus{
					{Pod: primaryPod, IsPrimary: true, IsPodReady: false, Error: errors.New("startup failed")},
					{Pod: replicaPod, IsPodReady: true, ExecutableHash: "hash", IsWalReceiverActive: true},
				},
			}

			result, err := reconciler.reconcileOfflinePVCResize(ctx, cluster, resources, podList)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
			Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(primaryPod), &corev1.Pod{})).To(Succeed())
			Expect(k8sClient.Get(ctx, k8client.ObjectKeyFromObject(replicaPod), &corev1.Pod{})).To(Succeed())
		})
})
