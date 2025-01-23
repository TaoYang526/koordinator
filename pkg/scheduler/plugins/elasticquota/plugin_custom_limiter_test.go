package elasticquota

import (
	"context"
	"fmt"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/apis/config"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/koordinator-sh/koordinator/apis/extension"
	"github.com/koordinator-sh/koordinator/apis/thirdparty/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/elasticquota/core"
	"github.com/stretchr/testify/assert"
)

func TestPlugin_PreFilter_CustomLimiter(t *testing.T) {
	core.RegisterCustomLimiter(&core.MockCustomLimiter{})
	customKey := core.CustomKeyMock
	annotationKeyLimit := core.AnnotationKeyMockLimit
	annotationKeyArgs := core.AnnotationKeyMockArgs
	test := []struct {
		name            string
		pod             *corev1.Pod
		quotaInfo       *v1alpha1.ElasticQuota
		parentQuotaInfo *v1alpha1.ElasticQuota
		expectedStatus  framework.Status
	}{
		{
			name: "accept: without custom limit conf",
			pod: MakePod("t1-ns1", "pod1").Label(extension.LabelQuotaName, "test-child").Container(
				MakeResourceList().CPU(2).Mem(2).Obj()).Obj(),
			quotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-child",
					Labels: map[string]string{
						extension.LabelQuotaParent: "test",
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(1).Mem(1).Obj(),
				},
			},
			parentQuotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(5).Mem(5).Obj(),
				},
			},
			expectedStatus: *framework.NewStatus(framework.Success, ""),
		},
		{
			name: "accept: used <= limit",
			pod: MakePod("t1-ns1", "pod1").Label(extension.LabelQuotaName, "test-child").Container(
				MakeResourceList().CPU(2).Mem(2).Obj()).Obj(),
			quotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-child",
					Labels: map[string]string{
						extension.LabelQuotaParent: "test",
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(1).Mem(1).Obj(),
				},
			},
			parentQuotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
					Annotations: map[string]string{
						annotationKeyLimit: `{"cpu":2,"memory":2}`,
						annotationKeyArgs:  `0.5`,
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(5).Mem(5).Obj(),
				},
			},
			expectedStatus: *framework.NewStatus(framework.Success, ""),
		},
		{
			name: "ignore invalid limit for limiter mock",
			pod: MakePod("t1-ns1", "pod1").Label(extension.LabelQuotaName, "test-child").Container(
				MakeResourceList().CPU(2).Mem(2).Obj()).Obj(),
			quotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-child",
					Labels: map[string]string{
						extension.LabelQuotaParent: "test",
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(1).Mem(1).Obj(),
				},
			},
			parentQuotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
					Annotations: map[string]string{
						annotationKeyLimit: `{`,
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(5).Mem(5).Obj(),
				},
			},
			expectedStatus: *framework.NewStatus(framework.Success, ""),
		},
		{
			name: "ignore invalid args for limiter mock",
			pod: MakePod("t1-ns1", "pod1").Label(extension.LabelQuotaName, "test-child").Container(
				MakeResourceList().CPU(2).Mem(2).Obj()).Obj(),
			quotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-child",
					Labels: map[string]string{
						extension.LabelQuotaParent: "test",
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(1).Mem(1).Obj(),
				},
			},
			parentQuotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
					Annotations: map[string]string{
						annotationKeyLimit: `{"cpu":2,"memory":2}`,
						annotationKeyArgs:  `{`,
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(5).Mem(5).Obj(),
				},
			},
			expectedStatus: *framework.NewStatus(framework.Success, ""),
		},
		{
			name: "reject when cpu reach the limit",
			pod: MakePod("t1-ns1", "pod1").Label(extension.LabelQuotaName, "test-child").Container(
				MakeResourceList().CPU(3).Mem(3).Obj()).Obj(),
			quotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-child",
					Labels: map[string]string{
						extension.LabelQuotaParent: "test",
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(1).Mem(1).Obj(),
				},
			},
			parentQuotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
					Annotations: map[string]string{
						annotationKeyLimit: `{"cpu":1,"memory":10}`,
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(30).Mem(30).Obj(),
					Min: MakeResourceList().CPU(5).Mem(5).Obj(),
				},
			},
			expectedStatus: *framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("check failed for quota %s by custom-limiter mock: "+
					"insufficient resource, limit=%v, used=%v, request=%v, exceededResourceNames=[cpu]",
					"test",
					printResourceList(MakeResourceList().CPU(1).Mem(10).Obj()),
					printResourceList(corev1.ResourceList{}),
					printResourceList(MakeResourceList().CPU(3).Mem(3).Obj()))),
		}, {
			name: "reject when memory reach the limit",
			pod: MakePod("t1-ns1", "pod1").Label(extension.LabelQuotaName, "test-child").Container(
				MakeResourceList().CPU(3).Mem(3).Obj()).Obj(),
			quotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-child",
					Labels: map[string]string{
						extension.LabelQuotaParent: "test",
					},
					Annotations: map[string]string{
						annotationKeyLimit: `{"cpu":10,"memory":1}`,
					},
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(10).Mem(10).Obj(),
					Min: MakeResourceList().CPU(1).Mem(1).Obj(),
				},
			},
			parentQuotaInfo: &v1alpha1.ElasticQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
				Spec: v1alpha1.ElasticQuotaSpec{
					Max: MakeResourceList().CPU(30).Mem(30).Obj(),
					Min: MakeResourceList().CPU(5).Mem(5).Obj(),
				},
			},
			expectedStatus: *framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("check failed for quota %s by custom-limiter mock: "+
					"insufficient resource, limit=%v, used=%v, request=%v, exceededResourceNames=[memory]",
					"test-child",
					printResourceList(MakeResourceList().CPU(10).Mem(1).Obj()),
					printResourceList(corev1.ResourceList{}),
					printResourceList(MakeResourceList().CPU(3).Mem(3).Obj()))),
		},
	}
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			suit := newPluginTestSuit(t, nil,
				func(elasticQuotaArgs *config.ElasticQuotaArgs) {
					elasticQuotaArgs.EnableRuntimeQuota = false
					elasticQuotaArgs.EnableCheckParentQuota = true
					elasticQuotaArgs.CustomLimiterKeys = []string{customKey}
				})
			p, err := suit.proxyNew(suit.elasticQuotaArgs, suit.Handle)
			assert.Nil(t, err)
			gp := p.(*Plugin)
			gp.pluginArgs.EnableCheckParentQuota = true
			gp.pluginArgs.CustomLimiterKeys = []string{customKey}
			gp.OnQuotaAdd(tt.parentQuotaInfo)
			gp.OnQuotaAdd(tt.quotaInfo)
			// verify
			state := framework.NewCycleState()
			_, status := gp.PreFilter(context.TODO(), state, tt.pod)
			assert.Equal(t, tt.expectedStatus, *status)
		})
	}
}
