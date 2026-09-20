/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.
SPDX-License-Identifier: Apache-2.0
*/
package persistentvolumeclaim

import (
	"context"
	"fmt"
	"testing"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	schemeBuilder "github.com/cloudnative-pg/cloudnative-pg/internal/scheme"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestRetiredReplicaVolumeReclamation(t *testing.T) {
	cases := []struct {
		name    string
		change  func(*apiv1.Cluster, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *corev1.Pod)
		refused bool
	}{
		{name: "owned replica"},
		{name: "already armed", change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *corev1.Pod) {
			pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
		}},
		{name: "primary selected", refused: true, change: func(c *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *corev1.Pod) {
			c.Status.CurrentPrimary = "db-2"
			c.Status.TargetPrimary = "db-2"
		}},
		{name: "switchover pending", refused: true, change: func(c *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *corev1.Pod) {
			c.Status.TargetPrimary = "db-2"
		}},
		{name: "scale down cancelled", refused: true, change: func(c *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *corev1.Pod) {
			c.Spec.Instances = 2
		}},
		{name: "unready cluster", refused: true, change: func(c *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *corev1.Pod) {
			c.Status.ReadyInstances = 1
		}},
		{name: "unready primary", refused: true, change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, p *corev1.Pod) {
			p.Status.Conditions = nil
		}},
		{name: "foreign claim owner", refused: true, change: func(_ *apiv1.Cluster, pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *corev1.Pod) {
			pvc.OwnerReferences[0].UID = "other-cluster"
		}},
		{name: "foreign primary owner", refused: true, change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, p *corev1.Pod) {
			p.OwnerReferences[0].UID = "other-cluster"
		}},
		{name: "rebound volume", refused: true, change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *corev1.Pod) {
			pv.Spec.ClaimRef.UID = "replacement-claim"
		}},
		{name: "wrong namespace", refused: true, change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *corev1.Pod) {
			pv.Spec.ClaimRef.Namespace = "other"
		}},
		{name: "wrong instance label", refused: true, change: func(_ *apiv1.Cluster, pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *corev1.Pod) {
			pvc.Labels[utils.InstanceNameLabelName] = "db-3"
		}},
		{name: "unbound volume", refused: true, change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *corev1.Pod) {
			pv.Status.Phase = corev1.VolumeReleased
		}},
		{name: "non CSI storage", refused: true, change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *corev1.Pod) {
			pv.Spec.CSI = nil
		}},
		{name: "wrong storage class", refused: true, change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *corev1.Pod) {
			pv.Spec.StorageClassName = "other"
		}},
		{name: "mounted by primary", refused: true, change: func(_ *apiv1.Cluster, _ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, p *corev1.Pod) {
			p.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "db-2"}}}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, pvc, pv, primary := retiredFixture()
			if tc.change != nil {
				tc.change(c, pvc, pv, primary)
			}
			untouched := pv.DeepCopy()
			untouched.Name = "unrelated-retained-volume"
			untouched.UID = "other-volume"
			untouched.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
			cl := fake.NewClientBuilder().WithScheme(schemeBuilder.BuildWithAllKnownScheme()).WithObjects(c, pvc, pv, primary, untouched).Build()
			err := DeleteRetiredReplicaPVCGroup(ctx, cl, cl, c, "db-2")
			if (err != nil) != tc.refused {
				t.Fatalf("refused=%v, error=%v", tc.refused, err)
			}
			got := &corev1.PersistentVolume{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(pv), got); err != nil {
				t.Fatal(err)
			}
			expected := corev1.PersistentVolumeReclaimDelete
			if tc.refused {
				expected = pv.Spec.PersistentVolumeReclaimPolicy
			}
			if got.Spec.PersistentVolumeReclaimPolicy != expected {
				t.Fatalf("policy=%s expected=%s", got.Spec.PersistentVolumeReclaimPolicy, expected)
			}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(untouched), got); err != nil {
				t.Fatal(err)
			}
			if got.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
				t.Fatal("unrelated volume modified")
			}
			err = cl.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{})
			if tc.refused && err != nil {
				t.Fatal("refused cleanup must retain the claim", err)
			}
			if !tc.refused && !apierrs.IsNotFound(err) {
				t.Fatal("approved cleanup must delete the verified claim", err)
			}

		})
	}
}

func TestRetiredVolumeReclamationIsOptIn(t *testing.T) {
	c, _, _, _ := retiredFixture()
	c.Annotations = nil
	// No client: a non-opted-in cluster must not require PV permissions or reads.
	if err := DeleteRetiredReplicaPVCGroup(context.Background(), nil, nil, c, "db-2"); err != nil {
		t.Fatal(err)
	}
}

func TestRetiredVolumeReclamationRejectsStaleCluster(t *testing.T) {
	c, pvc, pv, primary := retiredFixture()
	current := c.DeepCopy()
	current.Generation++
	cl := fake.NewClientBuilder().WithScheme(schemeBuilder.BuildWithAllKnownScheme()).WithObjects(current, pvc, pv, primary).Build()
	if err := DeleteRetiredReplicaPVCGroup(context.Background(), cl, cl, c, "db-2"); err == nil {
		t.Fatal("accepted stale scale-down decision")
	}
}

func retiredFixture() (*apiv1.Cluster, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *corev1.Pod) {
	controller := true
	storageClass := "fp-block-generic"
	c := &apiv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "test", UID: "cluster-uid", Generation: 3, Annotations: map[string]string{ReclaimRetiredReplicaVolumesAnnotation: "true"}}, Spec: apiv1.ClusterSpec{Instances: 1}, Status: apiv1.ClusterStatus{Instances: 2, ReadyInstances: 2, CurrentPrimary: "db-1", TargetPrimary: "db-1"}}
	owner := metav1.OwnerReference{APIVersion: "postgresql.cnpg.io/v1", Kind: "Cluster", Name: c.Name, UID: c.UID, Controller: &controller}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "db-2", Namespace: c.Namespace, UID: types.UID("claim-uid"), OwnerReferences: []metav1.OwnerReference{owner}, Labels: map[string]string{utils.ClusterLabelName: c.Name, utils.InstanceNameLabelName: "db-2"}}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "retired-volume", StorageClassName: &storageClass}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "retired-volume", UID: "volume-uid"}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "csi.exoscale.com", VolumeHandle: "zone/volume"}}, ClaimRef: &corev1.ObjectReference{Name: pvc.Name, Namespace: pvc.Namespace, UID: pvc.UID}, StorageClassName: storageClass, PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	primary := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "db-1", Namespace: c.Namespace, OwnerReferences: []metav1.OwnerReference{owner}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	return c, pvc, pv, primary
}

func TestRetiredVolumeReclamationPreservesRecreatedClaim(t *testing.T) {
	c, pvc, pv, primary := retiredFixture()
	cl := fake.NewClientBuilder().WithScheme(schemeBuilder.BuildWithAllKnownScheme()).WithObjects(c, pvc, pv, primary).
		WithInterceptorFuncs(interceptor.Funcs{Delete: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if claim, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				if err := inner.Delete(ctx, claim); err != nil {
					return err
				}
				replacement := claim.DeepCopy()
				replacement.UID = "replacement-claim"
				replacement.ResourceVersion = ""
				if err := inner.Create(ctx, replacement); err != nil {
					return err
				}
				// The fake client does not enforce UID preconditions. Model the
				// API-server boundary and assert the request carries the old UID.
				options := &client.DeleteOptions{}
				for _, option := range opts {
					option.ApplyToDelete(options)
				}
				if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != claim.UID {
					t.Fatal("missing original claim UID precondition")
				}
				return apierrs.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, claim.Name, fmt.Errorf("UID changed"))
			}
			return inner.Delete(ctx, obj, opts...)
		}}).Build()
	if err := DeleteRetiredReplicaPVCGroup(context.Background(), cl, cl, c, "db-2"); !apierrs.IsConflict(err) {
		t.Fatalf("expected UID conflict, got %v", err)
	}
	got := &corev1.PersistentVolumeClaim{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(pvc), got); err != nil {
		t.Fatal(err)
	}
	if got.UID != "replacement-claim" {
		t.Fatal("replacement claim lost")
	}
}
