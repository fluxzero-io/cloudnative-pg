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
	"fmt"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/postgres"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/reconciler/persistentvolumeclaim"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
)

const (
	phaseOfflinePVCResize        = "Offline PVC resize in progress"
	phaseOfflinePVCResizeDelayed = "Offline PVC resize delayed"

	offlinePVCResizeReason                      = "offline PVC resize requires the instance Pod to be stopped"
	offlinePVCResizeSwitchoverMaximumLagInBytes = 16 * 1024 * 1024
)

func (r *ClusterReconciler) reconcileOfflinePVCResize(
	ctx context.Context,
	cluster *apiv1.Cluster,
	resources *managedResources,
	instancesStatus postgres.PostgresqlStatusList,
) (ctrl.Result, error) {
	if offlinePVCResizeBlockedByClusterState(cluster) {
		return ctrl.Result{}, nil
	}

	if res, err := r.reconcileNonReadyOfflineResizePod(ctx, cluster, resources, instancesStatus); !res.IsZero() || err != nil {
		return res, err
	}

	// From here on, offline resize behaves like a controlled rollout and should
	// only act when the cluster topology is stable.
	if cluster.Status.Instances != cluster.Spec.Instances ||
		cluster.Status.ReadyInstances != cluster.Status.Instances ||
		cluster.Status.ReadyInstances != len(instancesStatus.Items) {
		return ctrl.Result{}, nil
	}

	if res, err := r.reconcileReadyOfflineResizeReplica(ctx, cluster, resources.pvcs.Items, instancesStatus); !res.IsZero() || err != nil {
		return res, err
	}

	return r.reconcilePrimaryOfflineResize(ctx, cluster, resources.pvcs.Items, instancesStatus)
}

func offlinePVCResizeBlockedByClusterState(cluster *apiv1.Cluster) bool {
	if cluster.Status.CurrentPrimary != "" &&
		cluster.Status.TargetPrimary != "" &&
		cluster.Status.CurrentPrimary != cluster.Status.TargetPrimary {
		return true
	}

	if cluster.Status.Phase == apiv1.PhaseUnrecoverable {
		return true
	}

	return cluster.Annotations[utils.HibernationAnnotationName] == string(utils.HibernationAnnotationValueOn)
}

func (r *ClusterReconciler) reconcileNonReadyOfflineResizePod(
	ctx context.Context,
	cluster *apiv1.Cluster,
	resources *managedResources,
	instancesStatus postgres.PostgresqlStatusList,
) (ctrl.Result, error) {
	for idx := range resources.instances.Items {
		pod := &resources.instances.Items[idx]
		if pod.DeletionTimestamp != nil ||
			cluster.IsInstanceFenced(pod.Name) ||
			!persistentvolumeclaim.IsOfflineResizePendingForPod(cluster, pod, resources.pvcs.Items) ||
			isPodReportingReady(instancesStatus, pod.Name) {
			continue
		}

		if pod.Name == cluster.Status.CurrentPrimary && cluster.Status.Instances > 1 {
			continue
		}

		return r.stopPodForOfflinePVCResize(ctx, cluster, pod)
	}

	return ctrl.Result{}, nil
}

func (r *ClusterReconciler) reconcileReadyOfflineResizeReplica(
	ctx context.Context,
	cluster *apiv1.Cluster,
	pvcs []corev1.PersistentVolumeClaim,
	instancesStatus postgres.PostgresqlStatusList,
) (ctrl.Result, error) {
	for idx := len(instancesStatus.Items) - 1; idx >= 0; idx-- {
		status := instancesStatus.Items[idx]
		if status.Pod == nil ||
			status.Pod.Name == cluster.Status.CurrentPrimary ||
			cluster.IsInstanceFenced(status.Pod.Name) ||
			!status.IsPodReady ||
			status.MightBeUnavailable ||
			!persistentvolumeclaim.IsOfflineResizePendingForPod(cluster, status.Pod, pvcs) {
			continue
		}

		return r.stopPodForOfflinePVCResize(ctx, cluster, status.Pod)
	}

	return ctrl.Result{}, nil
}

func (r *ClusterReconciler) reconcilePrimaryOfflineResize(
	ctx context.Context,
	cluster *apiv1.Cluster,
	pvcs []corev1.PersistentVolumeClaim,
	instancesStatus postgres.PostgresqlStatusList,
) (ctrl.Result, error) {
	primaryStatus := getCurrentPrimaryStatus(cluster, instancesStatus)
	if primaryStatus == nil ||
		primaryStatus.Pod == nil ||
		cluster.IsInstanceFenced(primaryStatus.Pod.Name) ||
		!primaryStatus.IsPodReady ||
		primaryStatus.MightBeUnavailable ||
		!persistentvolumeclaim.IsOfflineResizePendingForPod(cluster, primaryStatus.Pod, pvcs) {
		return ctrl.Result{}, nil
	}

	if cluster.Status.Instances <= 1 {
		return r.stopPodForOfflinePVCResize(ctx, cluster, primaryStatus.Pod)
	}

	if cluster.GetPrimaryUpdateStrategy() == apiv1.PrimaryUpdateStrategySupervised {
		log.FromContext(ctx).Info("Waiting for the user to request a switchover to complete offline PVC resize",
			"primaryPod", primaryStatus.Pod.Name)
		if err := r.RegisterPhase(ctx, cluster, apiv1.PhaseWaitingForUser,
			"User must issue a supervised switchover"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, ErrNextLoop
	}

	target := findOfflineResizeSwitchoverTarget(cluster, instancesStatus, primaryStatus, pvcs)
	if target == nil {
		log.FromContext(ctx).Info(
			"Waiting for a healthy resized replica before stopping the primary for offline PVC resize",
			"primaryPod", primaryStatus.Pod.Name,
		)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	managerResult := r.rolloutManager.CoordinateRollout(
		client.ObjectKeyFromObject(cluster),
		primaryStatus.Pod.Name,
	)
	if !managerResult.RolloutAllowed {
		return r.delayOfflinePVCResize(ctx, cluster, primaryStatus.Pod.Name, managerResult.TimeToWait)
	}

	r.Recorder.Eventf(cluster, "Normal", "OfflineResizeSwitchover",
		"Initiating switchover to %s before stopping %s for offline PVC resize",
		target.Pod.Name,
		primaryStatus.Pod.Name)
	if err := r.RegisterPhase(ctx, cluster, phaseOfflinePVCResize,
		"Switching over before stopping the primary for offline PVC resize"); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.setPrimaryInstance(ctx, cluster, target.Pod.Name); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *ClusterReconciler) stopPodForOfflinePVCResize(
	ctx context.Context,
	cluster *apiv1.Cluster,
	pod *corev1.Pod,
) (ctrl.Result, error) {
	managerResult := r.rolloutManager.CoordinateRollout(client.ObjectKeyFromObject(cluster), pod.Name)
	if !managerResult.RolloutAllowed {
		return r.delayOfflinePVCResize(ctx, cluster, pod.Name, managerResult.TimeToWait)
	}

	reason := fmt.Sprintf("%s: %s", offlinePVCResizeReason, pod.Name)
	if err := r.RegisterPhase(ctx, cluster, phaseOfflinePVCResize, reason); err != nil {
		return ctrl.Result{}, err
	}

	log.FromContext(ctx).Info("Stopping instance pod for offline PVC resize",
		"pod", pod.Name,
		"reason", reason,
	)
	r.Recorder.Eventf(cluster, "Normal", "OfflineResizeInstance",
		"Stopping instance %v for offline PVC resize", pod.Name)

	if err := r.Delete(ctx, pod); err != nil && !apierrs.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *ClusterReconciler) delayOfflinePVCResize(
	ctx context.Context,
	cluster *apiv1.Cluster,
	podName string,
	timeToWait time.Duration,
) (ctrl.Result, error) {
	r.Recorder.Eventf(
		cluster,
		"Normal",
		"OfflineResizeDelayed",
		"Offline PVC resize of pod %s has been delayed for %s",
		podName,
		timeToWait.String(),
	)
	if err := r.RegisterPhase(
		ctx,
		cluster,
		phaseOfflinePVCResizeDelayed,
		"The cluster needs an offline PVC resize, but the operator is configured to delay the operation",
	); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func canIgnoreFullDiskDuringOfflineResize(
	cluster *apiv1.Cluster,
	status postgres.PostgresqlStatus,
	instances postgres.PostgresqlStatusList,
	pvcs []corev1.PersistentVolumeClaim,
) bool {
	if status.Pod == nil || !persistentvolumeclaim.IsOfflineResizePendingForPod(cluster, status.Pod, pvcs) {
		return false
	}

	isPrimary := status.Pod.Name == cluster.Status.CurrentPrimary || status.IsPrimary
	if !isPrimary {
		return true
	}

	if cluster.Status.Instances <= 1 {
		return true
	}

	return findOfflineResizeSwitchoverTarget(cluster, instances, &status, pvcs) != nil
}

func getCurrentPrimaryStatus(
	cluster *apiv1.Cluster,
	instancesStatus postgres.PostgresqlStatusList,
) *postgres.PostgresqlStatus {
	for idx := range instancesStatus.Items {
		status := &instancesStatus.Items[idx]
		if status.Pod != nil && status.Pod.Name == cluster.Status.CurrentPrimary {
			return status
		}
	}

	return nil
}

func isPodReportingReady(instancesStatus postgres.PostgresqlStatusList, podName string) bool {
	for idx := range instancesStatus.Items {
		status := instancesStatus.Items[idx]
		if status.Pod == nil || status.Pod.Name != podName {
			continue
		}

		return status.IsPodReady && status.Error == nil
	}

	return false
}

func findOfflineResizeSwitchoverTarget(
	cluster *apiv1.Cluster,
	podList postgres.PostgresqlStatusList,
	primaryStatus *postgres.PostgresqlStatus,
	pvcs []corev1.PersistentVolumeClaim,
) *postgres.PostgresqlStatus {
	for idx := range podList.Items {
		candidate := &podList.Items[idx]
		if candidate.Pod == nil || isPrimaryStatus(candidate, primaryStatus) {
			continue
		}

		if cluster.IsInstanceFenced(candidate.Pod.Name) ||
			!candidate.HasHTTPStatus() ||
			candidate.MightBeUnavailable ||
			!candidate.IsWalReceiverActive ||
			!hasAcceptableOfflineResizeSwitchoverLag(candidate, primaryStatus) ||
			persistentvolumeclaim.IsOfflineResizePendingForPod(cluster, candidate.Pod, pvcs) {
			continue
		}

		return candidate
	}

	return nil
}

func isPrimaryStatus(candidate, primaryStatus *postgres.PostgresqlStatus) bool {
	return primaryStatus != nil &&
		primaryStatus.Pod != nil &&
		candidate.Pod.Name == primaryStatus.Pod.Name
}

func hasAcceptableOfflineResizeSwitchoverLag(
	candidate *postgres.PostgresqlStatus,
	primaryStatus *postgres.PostgresqlStatus,
) bool {
	if candidate.ReceivedLsn == "" || candidate.ReplayLsn == "" {
		return false
	}

	receivedLsn, err := candidate.ReceivedLsn.Parse()
	if err != nil {
		return false
	}
	replayLsn, err := candidate.ReplayLsn.Parse()
	if err != nil {
		return false
	}
	if replayLsn > receivedLsn {
		receivedLsn = replayLsn
	}
	if receivedLsn-replayLsn > offlinePVCResizeSwitchoverMaximumLagInBytes {
		return false
	}

	if primaryStatus == nil || primaryStatus.CurrentLsn == "" {
		return true
	}

	currentLsn, err := primaryStatus.CurrentLsn.Parse()
	if err != nil {
		return false
	}
	if replayLsn > currentLsn {
		return true
	}
	return currentLsn-replayLsn <= offlinePVCResizeSwitchoverMaximumLagInBytes
}
