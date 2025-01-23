package core

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"

	"github.com/koordinator-sh/koordinator/apis/thirdparty/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/util"
)

const (
	CustomKeyMock          = "mock"
	AnnotationKeyMockLimit = "mock-limit"
	AnnotationKeyMockArgs  = "mock-args"
)

type MockCustomLimiter struct {
}

func (m *MockCustomLimiter) GetKey() string {
	return CustomKeyMock
}

func (m *MockCustomLimiter) GetLimitAndArgs(quota *v1alpha1.ElasticQuota) (limit corev1.ResourceList,
	args CustomArgs, err error) {
	if mockLimitConfV := quota.Annotations[AnnotationKeyMockLimit]; mockLimitConfV != "" {
		if err = json.Unmarshal([]byte(mockLimitConfV), &limit); err != nil {
			err = fmt.Errorf("failed to unmarshal limit for custom limiter %s, err=%v", m.GetKey(), err)
			return
		}
	}
	var mockArgs MockCustomArgs
	if mockArgsConfV := quota.Annotations[AnnotationKeyMockArgs]; mockArgsConfV != "" {
		if err = json.Unmarshal([]byte(mockArgsConfV), &mockArgs); err != nil {
			err = fmt.Errorf("failed to unmarshal args for custom limiter %s, err=%v", m.GetKey(), err)
			return
		}
		args = &mockArgs
	}
	return
}

func (m *MockCustomLimiter) UpdateUsed(quotaInfo *QuotaInfo, requestDelta corev1.ResourceList,
	_ *CustomLimiterState) {
	curUsed := quotaInfo.CalculateInfo.CustomUsed[m.GetKey()]
	quotaInfo.CalculateInfo.CustomUsed[m.GetKey()] = quotav1.Add(curUsed, requestDelta)
}

func (m *MockCustomLimiter) Check(quotaInfo *QuotaInfo, requestDelta corev1.ResourceList,
	_ *CustomLimiterState) error {
	curLimit := quotaInfo.CalculateInfo.CustomLimits[m.GetKey()]
	if quotav1.IsZero(curLimit) {
		return nil
	}
	curUsed := quotaInfo.CalculateInfo.CustomUsed[m.GetKey()]
	newUsed := quotav1.Add(curUsed, requestDelta)
	if notExceeded, exceededResourceNames := quotav1.LessThanOrEqual(newUsed, curLimit); !notExceeded {
		return fmt.Errorf(
			"insufficient resource, limit=%v, used=%v, request=%v, exceededResourceNames=%v",
			util.PrintResourceList(curLimit), util.PrintResourceList(curUsed),
			util.PrintResourceList(requestDelta), exceededResourceNames)
	}
	args := quotaInfo.CalculateInfo.CustomArgsMap[m.GetKey()]
	if args != nil {
		if args.(*MockCustomArgs).Ratio > 1 {
			return fmt.Errorf("ratio should not greater than 1 but got %v", args.(*MockCustomArgs).Ratio)
		}
	}
	return nil
}

type MockCustomArgs struct {
	Ratio float64 `json:"ratio"`
}

func (mca *MockCustomArgs) DeepCopy() CustomArgs {
	return &MockCustomArgs{
		Ratio: mca.Ratio,
	}
}

func (mca *MockCustomArgs) DeepEquals(another CustomArgs) bool {
	if mca == nil && another == nil {
		return true
	}
	if mca == nil || another == nil {
		return false
	}
	return mca.Ratio == another.(*MockCustomArgs).Ratio
}
