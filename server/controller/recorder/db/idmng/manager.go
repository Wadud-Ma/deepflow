/**
 * Copyright (c) 2024 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package idmng

import (
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/op/go-logging"

	ctrlrcommon "github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/db/mysql"
	"github.com/deepflowio/deepflow/server/controller/db/mysql/query"
	"github.com/deepflowio/deepflow/server/controller/recorder/common"
	. "github.com/deepflowio/deepflow/server/controller/recorder/config"
	. "github.com/deepflowio/deepflow/server/controller/recorder/constraint"
)

var log = logging.MustGetLogger("recorder.idmng")

var minID = 1

type IDManager struct {
	org *common.ORG

	resourceTypeToIDPool map[string]IDPoolUpdater
}

func newIDManager(cfg RecorderConfig, orgID int) (*IDManager, error) {
	log.Infof("create id manager for org: %d", orgID)
	org, err := common.NewORG(orgID)
	if err != nil {
		log.Errorf("failed to create org object: %s", err.Error())
		return nil, err
	}
	mng := &IDManager{org: org}
	mng.resourceTypeToIDPool = map[string]IDPoolUpdater{
		ctrlrcommon.RESOURCE_TYPE_REGION_EN:        newIDPool[mysql.Region](mng.org, ctrlrcommon.RESOURCE_TYPE_REGION_EN, cfg.ResourceMaxID0),
		ctrlrcommon.RESOURCE_TYPE_AZ_EN:            newIDPool[mysql.AZ](mng.org, ctrlrcommon.RESOURCE_TYPE_AZ_EN, cfg.ResourceMaxID0),
		ctrlrcommon.RESOURCE_TYPE_HOST_EN:          newIDPool[mysql.Host](mng.org, ctrlrcommon.RESOURCE_TYPE_HOST_EN, cfg.ResourceMaxID0),
		ctrlrcommon.RESOURCE_TYPE_VPC_EN:           newIDPool[mysql.VPC](mng.org, ctrlrcommon.RESOURCE_TYPE_VPC_EN, cfg.ResourceMaxID0),
		ctrlrcommon.RESOURCE_TYPE_NETWORK_EN:       newIDPool[mysql.Network](mng.org, ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, cfg.ResourceMaxID0),
		ctrlrcommon.RESOURCE_TYPE_POD_CLUSTER_EN:   newIDPool[mysql.PodCluster](mng.org, ctrlrcommon.RESOURCE_TYPE_POD_CLUSTER_EN, cfg.ResourceMaxID0),
		ctrlrcommon.RESOURCE_TYPE_POD_NAMESPACE_EN: newIDPool[mysql.PodNamespace](mng.org, ctrlrcommon.RESOURCE_TYPE_POD_NAMESPACE_EN, cfg.ResourceMaxID0),

		ctrlrcommon.RESOURCE_TYPE_VM_EN:              newIDPool[mysql.VM](mng.org, ctrlrcommon.RESOURCE_TYPE_VM_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_VROUTER_EN:         newIDPool[mysql.VRouter](mng.org, ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN:       newIDPool[mysql.DHCPPort](mng.org, ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN:    newIDPool[mysql.RDSInstance](mng.org, ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN:  newIDPool[mysql.RedisInstance](mng.org, ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN:     newIDPool[mysql.NATGateway](mng.org, ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_LB_EN:              newIDPool[mysql.LB](mng.org, ctrlrcommon.RESOURCE_TYPE_LB_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN:        newIDPool[mysql.PodNode](mng.org, ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN:     newIDPool[mysql.PodService](mng.org, ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_POD_EN:             newIDPool[mysql.Pod](mng.org, ctrlrcommon.RESOURCE_TYPE_POD_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_EN:     newIDPool[mysql.PodIngress](mng.org, ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN:       newIDPool[mysql.PodGroup](mng.org, ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_POD_REPLICA_SET_EN: newIDPool[mysql.PodReplicaSet](mng.org, ctrlrcommon.RESOURCE_TYPE_POD_REPLICA_SET_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_PROCESS_EN:         newIDPool[mysql.Process](mng.org, ctrlrcommon.RESOURCE_TYPE_PROCESS_EN, cfg.ResourceMaxID1),
		ctrlrcommon.RESOURCE_TYPE_VTAP_EN:            newIDPool[mysql.VTap](mng.org, ctrlrcommon.RESOURCE_TYPE_VTAP_EN, cfg.ResourceMaxID0),
	}
	return mng, nil
}

func (m *IDManager) Refresh() error {
	log.Info(m.org.Logf("refresh id pools started"))
	defer log.Info(m.org.Logf("refresh id pools completed"))

	var result error
	for _, idPool := range m.resourceTypeToIDPool {
		err := idPool.refresh()
		if err != nil {
			result = err
		}
	}
	return result
}

func (m *IDManager) AllocateIDs(resourceType string, count int) []int {
	idPool, ok := m.resourceTypeToIDPool[resourceType]
	if !ok {
		log.Error(m.org.Logf("resource type (%s) does not need to allocate id", resourceType))
		return []int{}
	}
	ids, _ := idPool.allocate(count)
	return ids
}

func (m *IDManager) RecycleIDs(resourceType string, ids []int) {
	idPool, ok := m.resourceTypeToIDPool[resourceType]
	if !ok {
		log.Error(m.org.Logf("resource type (%s) does not need to allocate id", resourceType))
		return
	}
	idPool.recycle(ids)
	return
}

type IDPoolUpdater interface {
	refresh() error
	allocate(count int) ([]int, error)
	recycle(ids []int)
}

// 缓存资源可用于分配的ID，提供ID的刷新、分配、回收接口
type IDPool[MT MySQLModel] struct {
	mutex sync.RWMutex
	AscIDAllocator
}

func newIDPool[MT MySQLModel](org *common.ORG, resourceType string, max int) *IDPool[MT] {
	p := &IDPool[MT]{
		AscIDAllocator: NewAscIDAllocator(org, resourceType, minID, max),
	}
	p.SetInUseIDsProvider(p)
	return p
}

func (p *IDPool[MT]) load() (mapset.Set[int], error) {
	items, err := query.FindInBatches[MT](p.org.DB.Unscoped().Select("id"))
	if err != nil {
		log.Error(p.org.Logf("failed to query %s: %v", p.resourceType, err))
		return nil, err
	}
	inUseIDsSet := mapset.NewSet[int]()
	for _, item := range items {
		inUseIDsSet.Add((*item).GetID())
	}
	log.Info(p.org.Logf("loaded %s ids successfully", p.resourceType))
	return inUseIDsSet, nil
}

func (p *IDPool[MT]) check(ids []int) ([]int, error) {
	var dbItems []*MT
	err := p.org.DB.Unscoped().Where("id IN ?", ids).Find(&dbItems).Error
	if err != nil {
		log.Error(p.org.Logf("failed to query %s: %v", p.resourceType, err))
		return nil, err
	}
	inUseIDs := make([]int, 0)
	if len(dbItems) != 0 {
		for _, item := range dbItems {
			inUseIDs = append(inUseIDs, (*item).GetID())
		}
		log.Info(p.org.Logf("%s ids: %+v are in use.", p.resourceType, inUseIDs))
	}
	return inUseIDs, nil
}

func (p *IDPool[MT]) refresh() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.Refresh()
}

// 批量分配ID，若ID池中数量不足，分配ID池所有ID；反之分配指定个数ID。
// 分配的ID中，若已有被实际使用的ID（闭源页面创建使用），排除已使用ID，仅分配剩余部分。
func (p *IDPool[MT]) allocate(count int) (ids []int, err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.Allocate(count)
}

func (p *IDPool[MT]) recycle(ids []int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.Recycle(ids)
}
