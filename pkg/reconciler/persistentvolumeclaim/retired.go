/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.
SPDX-License-Identifier: Apache-2.0
*/

package persistentvolumeclaim

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
)

// ReclaimRetiredReplicaVolumesAnnotation opts into reclaiming the physical
// volumes of replicas deliberately removed by scale-down. It does not change
// retention during cluster deletion, maintenance, or recovery.
const ReclaimRetiredReplicaVolumesAnnotation = "cnpg.io/reclaim-retired-replica-volumes"

// DeleteRetiredReplicaPVCGroup arms only exactly bound, owned replica volumes
// for CSI reclamation and deletes their claims with UID preconditions.
// Reads bypass the informer cache; optimistic locking prevents rebinding races.
// The caller must be the serialized scale-down reconciler, not generic PVC GC.
func DeleteRetiredReplicaPVCGroup(
	ctx context.Context, c client.Client, reader client.Reader, cluster *apiv1.Cluster, instance string,
) error {
	if cluster.Annotations[ReclaimRetiredReplicaVolumesAnnotation] != "true" {
		return nil
	}
	current := &apiv1.Cluster{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
		return err
	}
	if current.UID == "" || current.UID != cluster.UID || current.Generation != cluster.Generation ||
		current.DeletionTimestamp != nil || current.Annotations[ReclaimRetiredReplicaVolumesAnnotation] != "true" ||
		current.Spec.Instances < 1 || current.Spec.Instances >= current.Status.Instances ||
		current.Status.ReadyInstances != current.Status.Instances || current.Status.CurrentPrimary == "" ||
		current.Status.CurrentPrimary != current.Status.TargetPrimary ||
		instance == current.Status.CurrentPrimary || current.IsInstanceFenced(instance) ||
		current.IsNodeMaintenanceWindowInProgress() ||
		current.Annotations[utils.HibernationAnnotationName] == string(utils.HibernationAnnotationValueOn) {
		return fmt.Errorf("refusing volume reclamation outside a stable replica scale-down")
	}
	primary := &corev1.Pod{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: current.Namespace, Name: current.Status.CurrentPrimary}, primary); err != nil {
		return err
	}
	if !metav1.IsControlledBy(primary, current) || !utils.IsPodReady(*primary) || primary.DeletionTimestamp != nil {
		return fmt.Errorf("refusing volume reclamation without a healthy owned primary")
	}
	// Validate the complete PVC group before arming any member (PGDATA, WAL,
	// and tablespaces). A missing member is an idempotent retry, not a PV scan.
	volumes := make([]*corev1.PersistentVolume, 0)
	claims := make([]*corev1.PersistentVolumeClaim, 0)
	for _, expected := range getExpectedPVCsFromCluster(current, instance) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: current.Namespace, Name: expected.name}, pvc); err != nil {
			if apierrs.IsNotFound(err) {
				continue
			}
			return err
		}
		for _, mounted := range primary.Spec.Volumes {
			if mounted.PersistentVolumeClaim != nil && mounted.PersistentVolumeClaim.ClaimName == pvc.Name {
				return fmt.Errorf("refusing to reclaim primary volume %s", pvc.Name)
			}
		}
		if pvc.UID == "" || !metav1.IsControlledBy(pvc, current) ||
			pvc.Labels[utils.ClusterLabelName] != current.Name ||
			pvc.Labels[utils.InstanceNameLabelName] != instance || pvc.Spec.VolumeName == "" ||
			pvc.Spec.StorageClassName == nil || pvc.Status.Phase != corev1.ClaimBound {
			return fmt.Errorf("refusing to reclaim unverified replica claim %s", pvc.Name)
		}
		pv := &corev1.PersistentVolume{}
		if err := reader.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			return err
		}
		ref := pv.Spec.ClaimRef
		if pv.UID == "" || pv.Spec.CSI == nil || ref == nil || ref.UID != pvc.UID ||
			ref.Name != pvc.Name || ref.Namespace != pvc.Namespace ||
			pv.Spec.StorageClassName != *pvc.Spec.StorageClassName || pv.Status.Phase != corev1.VolumeBound {
			return fmt.Errorf("refusing to reclaim volume %s with a different claim identity", pv.Name)
		}
		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain &&
			pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
			return fmt.Errorf("unsupported reclaim policy on volume %s", pv.Name)
		}
		volumes = append(volumes, pv)
		claims = append(claims, pvc)
	}
	for _, pv := range volumes {
		if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
			continue
		}
		patch := client.MergeFromWithOptions(pv.DeepCopy(), client.MergeFromWithOptimisticLock{})
		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
		if err := c.Patch(ctx, pv, patch); err != nil {
			return fmt.Errorf("arming retired replica volume %s: %w", pv.Name, err)
		}
	}
	for _, pvc := range claims {
		if err := c.Delete(ctx, pvc, client.Preconditions{UID: &pvc.UID}); err != nil && !apierrs.IsNotFound(err) {
			return fmt.Errorf("deleting retired replica claim %s: %w", pvc.Name, err)
		}
	}
	return nil
}
