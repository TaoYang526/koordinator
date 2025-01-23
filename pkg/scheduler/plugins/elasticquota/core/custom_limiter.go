package core

import (
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierror "k8s.io/apimachinery/pkg/util/errors"

	"github.com/koordinator-sh/koordinator/apis/thirdparty/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/apis/config"
)

// CustomLimiter provides a mechanism to extend custom limiters,
// which can execute custom logic to limit the resource usage of a quota.
// For example, we can have a custom limiter to limit the resource usage of specified pods.
type CustomLimiter interface {
	GetKey() string
	GetLimitAndArgs(quota *v1alpha1.ElasticQuota) (corev1.ResourceList, CustomArgs, error)
	UpdateUsed(quotaInfo *QuotaInfo, requestDelta corev1.ResourceList, quotaChainSharedState *CustomLimiterState)
	Check(quotaInfo *QuotaInfo, requestDelta corev1.ResourceList, quotaChainSharedState *CustomLimiterState) error
}

type CustomArgs interface {
	DeepCopy() CustomArgs
	DeepEquals(args CustomArgs) bool
}

// registeredCustomLimiters is a global map of registered custom limiters, keyed by custom limiter name.
var registeredCustomLimiters = map[string]CustomLimiter{}

func RegisterCustomLimiter(customLimiter CustomLimiter) {
	registeredCustomLimiters[customLimiter.GetKey()] = customLimiter
}

func GetRegisteredCustomLimiter(key string) (CustomLimiter, error) {
	limiter := registeredCustomLimiters[key]
	if limiter == nil {
		return nil, fmt.Errorf("custom limiter %s not found", key)
	}
	return limiter, nil
}

// CustomLimiterState provides a mechanism for custom-limiters to store and retrieve arbitrary data
// in the update or check process of a quota chain in a bottom-up traversal (from leaf to root).
// Storage is keyed with StateKey, and valued with StateData.
// StateData can be shared among different custom-limiters, also can be shared among different quotas
// belongs to the same quota chain in a bottom-up traversal.
type CustomLimiterState struct {
	Storage sync.Map
}

func NewCustomLimiterState() *CustomLimiterState {
	return &CustomLimiterState{}
}

func (cls *CustomLimiterState) GetResourceList(stateKey string) corev1.ResourceList {
	if v, ok := cls.Storage.Load(stateKey); ok {
		return v.(corev1.ResourceList)
	}
	return corev1.ResourceList{}
}

func GetCustomLimiters(pluginArgs *config.ElasticQuotaArgs) (rst map[string]CustomLimiter, err error) {
	rst = make(map[string]CustomLimiter)
	var errs []error
	for _, key := range pluginArgs.CustomLimiterKeys {
		registeredLimiter, err := GetRegisteredCustomLimiter(key)
		if err != nil {
			errs = append(errs, fmt.Errorf("custom limiter %s not found", key))
			continue
		}
		rst[key] = registeredLimiter
	}
	if len(errs) > 0 {
		return nil, apierror.NewAggregate(errs)
	}
	return rst, nil
}

type CustomArgsMap map[string]CustomArgs

func (ca CustomArgsMap) DeepCopy() CustomArgsMap {
	if ca == nil {
		return nil
	}
	rst := make(CustomArgsMap)
	for k, v := range ca {
		rst[k] = v.DeepCopy()
	}
	return rst
}

func CustomArgsDeepEqual(a, b CustomArgs) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.DeepEquals(b)
}

type CustomResourceLists map[string]corev1.ResourceList

func (qrmt CustomResourceLists) DeepCopy() CustomResourceLists {
	if qrmt == nil {
		return nil
	}
	target := make(CustomResourceLists)
	for k, v := range qrmt {
		target[k] = v.DeepCopy()
	}
	return target
}
