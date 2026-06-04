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

package e2e

import (
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/reconciler/persistentvolumeclaim"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
	"github.com/cloudnative-pg/cloudnative-pg/tests"
	"github.com/cloudnative-pg/cloudnative-pg/tests/utils/run"
	"github.com/cloudnative-pg/cloudnative-pg/tests/utils/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Test case for validating storage expansion
// with different storage providers in different k8s environments
var _ = Describe("Verify storage", Label(tests.LabelStorage), func() {
	const (
		sampleFile  = fixturesDir + "/storage_expansion/cluster-storage-expansion.yaml.template"
		clusterName = "storage-expansion"
		level       = tests.Lowest
	)
	// Initializing a global namespace variable to be used in each test case
	var namespace, namespacePrefix string

	BeforeEach(func() {
		if testLevelEnv.Depth < int(level) {
			Skip("Test depth is lower than the amount requested for this test")
		}
	})

	// Gathering default storage class requires to check whether the value
	// of 'allowVolumeExpansion' is true or false
	defaultStorageClass := os.Getenv("E2E_DEFAULT_STORAGE_CLASS")

	Context("can be expanded", func() {
		BeforeEach(func() {
			// Initializing namespace variable to be used in test case
			namespacePrefix = "storage-expansion-true"
			// Extracting bool value of AllowVolumeExpansion
			allowExpansion, err := storage.GetStorageAllowExpansion(
				env.Ctx, env.Client,
				defaultStorageClass,
			)
			Expect(err).ToNot(HaveOccurred())
			if (allowExpansion == nil) || (*allowExpansion == false) {
				Skip(fmt.Sprintf("AllowedVolumeExpansion is false on %v", defaultStorageClass))
			}
		})

		It("expands PVCs via online resize", func() {
			var err error
			// Creating namespace
			namespace, err = env.CreateUniqueTestNamespace(env.Ctx, env.Client, namespacePrefix)
			Expect(err).ToNot(HaveOccurred())
			// Creating a cluster with three nodes
			AssertCreateCluster(namespace, clusterName, sampleFile, env)
			OnlineResizePVC(namespace, clusterName)
		})
	})

	Context("can not be expanded", func() {
		BeforeEach(func() {
			// Initializing namespace variable to be used in test case
			namespacePrefix = "storage-expansion-false"
			// Extracting bool value of AllowVolumeExpansion
			allowExpansion, err := storage.GetStorageAllowExpansion(
				env.Ctx, env.Client,
				defaultStorageClass,
			)
			Expect(err).ToNot(HaveOccurred())
			if (allowExpansion != nil) && (*allowExpansion == true) {
				Skip(fmt.Sprintf("AllowedVolumeExpansion is true on %v", defaultStorageClass))
			}
		})
		It("expands PVCs via offline resize", func() {
			var err error
			// Creating namespace
			namespace, err = env.CreateUniqueTestNamespace(env.Ctx, env.Client, namespacePrefix)
			Expect(err).ToNot(HaveOccurred())
			AssertCreateCluster(namespace, clusterName, sampleFile, env)
			By("update cluster for resizeInUseVolumes as false", func() {
				// Updating cluster with 'resizeInUseVolumes' sets to 'false' in storage.
				// Check if operator does not return error
				Eventually(func() error {
					_, _, err = run.Unchecked("kubectl patch cluster " + clusterName + " -n " + namespace +
						" -p '{\"spec\":{\"storage\":{\"resizeInUseVolumes\":false}}}' --type=merge")
					if err != nil {
						return err
					}
					return nil
				}, 60, 5).Should(Succeed())
			})
			OfflineResizePVC(namespace, clusterName, 600)
		})
	})

	Context("requires offline expansion", Label(tests.LabelOfflineResize), func() {
		const (
			offlineResizeClusterName           = "offline-resize"
			offlineResizeSingleClusterName     = "offline-resize-single"
			offlineResizeDefaultStorageClass   = "cnpg-offline-resize"
			offlineResizeInstances             = 2
			offlineResizeSingleInstance        = 1
			offlineResizePVCsPerInstance       = 3
			offlineResizeSinglePVCsPerInstance = 1
			offlineResizeSampleFile            = fixturesDir + "/storage_expansion/cluster-offline-resize.yaml.template"
			offlineResizeSingleSampleFile      = fixturesDir + "/storage_expansion/cluster-offline-resize-single.yaml.template"
		)

		BeforeEach(func() {
			namespacePrefix = "storage-expansion-offline"
		})

		It("keeps podless PVCs detached until capacity catches up", func() {
			var err error
			namespace, err = env.CreateUniqueTestNamespace(env.Ctx, env.Client, namespacePrefix)
			Expect(err).ToNot(HaveOccurred())
			offlineResizeStorageClass := fmt.Sprintf("%s-%s", offlineResizeDefaultStorageClass, namespace)
			GinkgoT().Setenv("E2E_OFFLINE_RESIZE_STORAGE_CLASS", offlineResizeStorageClass)

			prepareOfflineResizeStorage(
				namespace,
				offlineResizeStorageClass,
				offlineResizeClusterName,
				offlineResizeInstances*offlineResizePVCsPerInstance,
			)

			AssertCreateCluster(namespace, offlineResizeClusterName, offlineResizeSampleFile, env)
			initialPrimary := getCurrentPrimary(namespace, offlineResizeClusterName)

			By("requesting larger volumes", func() {
				Eventually(func() error {
					return requestOfflineResize(namespace, offlineResizeClusterName, "2Gi")
				}, 60, 5).Should(Succeed())
			})

			By("observing at least one PVC waiting for offline expansion", func() {
				Eventually(func(g Gomega) {
					g.Expect(offlineResizePendingInstances(g, namespace, offlineResizeClusterName)).ToNot(BeEmpty())
				}, 300, 2).Should(Succeed())
			})

			expandedInstances := completeDetachedOfflineResizes(namespace, offlineResizeInstances*offlineResizePVCsPerInstance)
			Expect(expandedInstances).To(HaveLen(offlineResizeInstances))
			Expect(expandedInstances[0]).ToNot(Equal(initialPrimary))

			AssertClusterIsReady(namespace, offlineResizeClusterName, 600, env)
			Expect(getCurrentPrimary(namespace, offlineResizeClusterName)).To(Equal(expandedInstances[0]))

			By("verifying every PVC reached the requested capacity", func() {
				Eventually(func(g Gomega) {
					pvcList := &corev1.PersistentVolumeClaimList{}
					g.Expect(env.Client.List(env.Ctx, pvcList, ctrlclient.InNamespace(namespace))).To(Succeed())
					g.Expect(pvcList.Items).To(HaveLen(offlineResizeInstances * offlineResizePVCsPerInstance))
					for _, pvc := range pvcList.Items {
						g.Expect(pvc.Status.Capacity.Storage().String()).To(Equal("2Gi"))
					}
				}, 300, 5).Should(Succeed())
			})
		})

		It("handles single-instance clusters with downtime", func() {
			var err error
			namespace, err = env.CreateUniqueTestNamespace(env.Ctx, env.Client, namespacePrefix)
			Expect(err).ToNot(HaveOccurred())
			offlineResizeStorageClass := fmt.Sprintf("%s-%s", offlineResizeDefaultStorageClass, namespace)
			GinkgoT().Setenv("E2E_OFFLINE_RESIZE_STORAGE_CLASS", offlineResizeStorageClass)

			prepareOfflineResizeStorage(
				namespace,
				offlineResizeStorageClass,
				offlineResizeSingleClusterName,
				offlineResizeSingleInstance*offlineResizeSinglePVCsPerInstance,
			)

			AssertCreateCluster(namespace, offlineResizeSingleClusterName, offlineResizeSingleSampleFile, env)
			initialPrimary := getCurrentPrimary(namespace, offlineResizeSingleClusterName)

			By("requesting a larger PGDATA volume", func() {
				Eventually(func() error {
					return requestOfflineResize(namespace, offlineResizeSingleClusterName, "2Gi")
				}, 60, 5).Should(Succeed())
			})

			By("observing the only PVC waiting for offline expansion", func() {
				Eventually(func(g Gomega) {
					g.Expect(offlineResizePendingInstances(g, namespace, offlineResizeSingleClusterName)).
						To(ConsistOf(initialPrimary))
				}, 300, 2).Should(Succeed())
			})

			expandedInstances := completeDetachedOfflineResizes(
				namespace,
				offlineResizeSingleInstance*offlineResizeSinglePVCsPerInstance,
			)
			Expect(expandedInstances).To(Equal([]string{initialPrimary}))

			AssertClusterIsReady(namespace, offlineResizeSingleClusterName, 600, env)
			Expect(getCurrentPrimary(namespace, offlineResizeSingleClusterName)).To(Equal(initialPrimary))

			By("verifying the single PGDATA PVC reached the requested capacity", func() {
				Eventually(func(g Gomega) {
					pvc := &corev1.PersistentVolumeClaim{}
					g.Expect(env.Client.Get(env.Ctx,
						ctrlclient.ObjectKey{Name: initialPrimary, Namespace: namespace},
						pvc)).To(Succeed())
					g.Expect(pvc.Status.Capacity.Storage().String()).To(Equal("2Gi"))
				}, 300, 5).Should(Succeed())
			})
		})
	})
})

func prepareOfflineResizeStorage(namespace, storageClassName, clusterName string, pvcCount int) {
	By("creating static hostPath PVs for offline resize simulation", func() {
		storageClass := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: storageClassName,
			},
			Provisioner:          "kubernetes.io/no-provisioner",
			AllowVolumeExpansion: ptr.To(true),
		}
		err := env.Client.Create(env.Ctx, storageClass)
		if err != nil && !apierrs.IsAlreadyExists(err) {
			Expect(err).ToNot(HaveOccurred())
		}

		hostPathType := corev1.HostPathDirectory
		nodeName := kindHostPathNodeName()
		for idx := 1; idx <= pvcCount; idx++ {
			hostPath := offlineResizeHostPath(namespace, clusterName, idx)
			prepareWritableKindHostPath(nodeName, hostPath)

			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("%s-%s-%d", storageClassName, namespace, idx),
				},
				Spec: corev1.PersistentVolumeSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					StorageClassName:              storageClassName,
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      corev1.LabelHostname,
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{nodeName},
										},
									},
								},
							},
						},
					},
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: hostPath,
							Type: &hostPathType,
						},
					},
				},
			}
			err = env.Client.Create(env.Ctx, pv)
			if err != nil && !apierrs.IsAlreadyExists(err) {
				Expect(err).ToNot(HaveOccurred())
			}
		}
	})
}

func offlineResizeHostPath(namespace, clusterName string, idx int) string {
	return fmt.Sprintf("/tmp/cnpg-offline-resize/%s/%s-%d", namespace, clusterName, idx)
}

func kindHostPathNodeName() string {
	if os.Getenv("TEST_CLOUD_VENDOR") != "kind" {
		Skip("offline resize e2e uses kind hostPath volumes")
	}

	clusterName := os.Getenv("CLUSTER_NAME")
	if clusterName == "" {
		stdout, _, err := run.Unchecked("kubectl config current-context")
		Expect(err).ToNot(HaveOccurred())
		clusterName = strings.TrimPrefix(strings.TrimSpace(stdout), "kind-")
	}

	stdout, _, err := run.Unchecked(fmt.Sprintf("kind get nodes --name %s", shellQuote(clusterName)))
	Expect(err).ToNot(HaveOccurred())
	nodeNames := strings.Fields(stdout)
	for _, nodeName := range nodeNames {
		if !strings.Contains(nodeName, "control-plane") {
			return nodeName
		}
	}
	Expect(nodeNames).ToNot(BeEmpty())
	return nodeNames[0]
}

func prepareWritableKindHostPath(nodeName, path string) {
	quotedPath := shellQuote(path)
	createPathCommand := fmt.Sprintf("mkdir -p %s && chown 26:26 %s && chmod 0770 %s",
		quotedPath, quotedPath, quotedPath)
	_, _, err := run.Unchecked(fmt.Sprintf(
		"docker exec %s sh -c %s",
		shellQuote(nodeName),
		shellQuote(createPathCommand),
	))
	Expect(err).ToNot(HaveOccurred())
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func getCurrentPrimary(namespace, clusterName string) string {
	cluster := &apiv1.Cluster{}
	Expect(env.Client.Get(env.Ctx,
		ctrlclient.ObjectKey{Name: clusterName, Namespace: namespace},
		cluster)).To(Succeed())
	return cluster.Status.CurrentPrimary
}

func requestOfflineResize(namespace, clusterName, size string) error {
	cluster := &apiv1.Cluster{}
	if err := env.Client.Get(env.Ctx,
		ctrlclient.ObjectKey{Name: clusterName, Namespace: namespace},
		cluster); err != nil {
		return err
	}

	original := cluster.DeepCopy()
	cluster.Spec.StorageConfiguration.Size = size
	if cluster.Spec.WalStorage != nil {
		cluster.Spec.WalStorage.Size = size
	}
	for idx := range cluster.Spec.Tablespaces {
		cluster.Spec.Tablespaces[idx].Storage.Size = size
	}

	return env.Client.Patch(env.Ctx, cluster, ctrlclient.MergeFrom(original))
}

func offlineResizePendingInstances(g Gomega, namespace, clusterName string) []string {
	cluster := &apiv1.Cluster{}
	g.Expect(env.Client.Get(env.Ctx,
		ctrlclient.ObjectKey{Name: clusterName, Namespace: namespace},
		cluster)).To(Succeed())

	pvcList := &corev1.PersistentVolumeClaimList{}
	g.Expect(env.Client.List(env.Ctx, pvcList, ctrlclient.InNamespace(namespace))).To(Succeed())

	pending := make(map[string]bool)
	instances := make([]string, 0)
	for _, pvc := range pvcList.Items {
		if !persistentvolumeclaim.IsOfflineResizePending(cluster, pvc) {
			continue
		}

		instanceName := pvc.Labels[utils.InstanceNameLabelName]
		if instanceName == "" {
			instanceName = pvc.Name
		}
		if !pending[instanceName] {
			pending[instanceName] = true
			instances = append(instances, instanceName)
		}
	}

	return instances
}

func completeDetachedOfflineResizes(namespace string, expectedPVCs int) []string {
	expanded := make(map[string]bool, expectedPVCs)
	expandedInstances := make([]string, 0, expectedPVCs)

	By("simulating controller-side expansion only after a PVC is detached", func() {
		Eventually(func(g Gomega) {
			podList := &corev1.PodList{}
			g.Expect(env.Client.List(env.Ctx, podList, ctrlclient.InNamespace(namespace))).To(Succeed())

			pvcList := &corev1.PersistentVolumeClaimList{}
			g.Expect(env.Client.List(env.Ctx, pvcList, ctrlclient.InNamespace(namespace))).To(Succeed())
			g.Expect(pvcList.Items).To(HaveLen(expectedPVCs))

			completed := 0
			for idx := range pvcList.Items {
				pvc := &pvcList.Items[idx]
				requested := pvc.Spec.Resources.Requests.Storage()
				if requested == nil || requested.IsZero() {
					continue
				}

				capacity := pvc.Status.Capacity.Storage()
				if capacity != nil && capacity.AsDec().Cmp(requested.AsDec()) >= 0 {
					completed++
					continue
				}

				if pvcUsedByActivePod(pvc.Name, podList.Items) {
					continue
				}

				expandPersistentVolume(g, pvc, requested.DeepCopy())
				expandPersistentVolumeClaimStatus(g, pvc, requested.DeepCopy())
				instanceName := pvc.Labels[utils.InstanceNameLabelName]
				if instanceName == "" {
					instanceName = pvc.Name
				}
				if !expanded[instanceName] {
					expanded[instanceName] = true
					expandedInstances = append(expandedInstances, instanceName)
				}
				completed++
			}

			g.Expect(completed).To(Equal(expectedPVCs))
		}, 600, 2).Should(Succeed())
	})

	return expandedInstances
}

func expandPersistentVolume(g Gomega, pvc *corev1.PersistentVolumeClaim, requested resource.Quantity) {
	if pvc.Spec.VolumeName == "" {
		return
	}

	pv := &corev1.PersistentVolume{}
	g.Expect(env.Client.Get(env.Ctx, ctrlclient.ObjectKey{Name: pvc.Spec.VolumeName}, pv)).To(Succeed())
	original := pv.DeepCopy()
	if pv.Spec.Capacity == nil {
		pv.Spec.Capacity = corev1.ResourceList{}
	}
	pv.Spec.Capacity[corev1.ResourceStorage] = requested
	g.Expect(env.Client.Patch(env.Ctx, pv, ctrlclient.MergeFrom(original))).To(Succeed())
}

func expandPersistentVolumeClaimStatus(g Gomega, pvc *corev1.PersistentVolumeClaim, requested resource.Quantity) {
	original := pvc.DeepCopy()
	if pvc.Status.Capacity == nil {
		pvc.Status.Capacity = corev1.ResourceList{}
	}
	pvc.Status.Capacity[corev1.ResourceStorage] = requested
	g.Expect(env.Client.Status().Patch(env.Ctx, pvc, ctrlclient.MergeFrom(original))).To(Succeed())
}

func pvcUsedByActivePod(pvcName string, pods []corev1.Pod) bool {
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil {
				continue
			}
			if volume.PersistentVolumeClaim.ClaimName == pvcName {
				return true
			}
		}
	}

	return false
}
