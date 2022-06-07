package clustermanager

import (
	"fmt"
	"reflect"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	kapi "k8s.io/api/core/v1"
	cache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type cmEventHandler struct {
	retry.EventHandler

	objType         reflect.Type
	cm              *ClusterManager
	extraParameters interface{}
	syncFunc        func([]interface{}) error
}

func (cm *ClusterManager) newRetryFramework(objectType reflect.Type) *retry.RetryFramework {
	resourceHandler := &retry.ResourceHandler{
		HasUpdateFunc:          hasResourceAnUpdateFunc(objectType),
		NeedsUpdateDuringRetry: false,
		ObjType:                objectType,
		EventHandler: &cmEventHandler{
			objType:         objectType,
			cm:              cm,
			extraParameters: nil,
			syncFunc:        nil,
		},
	}
	r := retry.NewRetryFramework(cm.stopChan, cm.wg, cm.watchFactory, resourceHandler)
	return r
}

// hasResourceAnUpdateFunc returns true if the given resource type has a dedicated update function.
// It returns false if, upon an update event on this resource type, we instead need to first delete the old
// object and then add the new one.
func hasResourceAnUpdateFunc(objType reflect.Type) bool {
	switch objType {
	case factory.NodeType:
		return true
	}
	return false
}

func (h *cmEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	// switch based on type
	switch h.objType {
	case factory.NodeType:
		node1, ok := obj1.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *kapi.Node", obj1)
		}
		node2, ok := obj2.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *kapi.Node", obj2)
		}

		// when shouldUpdateNode is false, the hostsubnet is not assigned by ovn-kubernetes
		shouldUpdate, err := shouldUpdateNode(node2, node1)
		if err != nil {
			klog.Errorf(err.Error())
		}
		return !shouldUpdate, nil
	}

	return false, nil
}

// Given an object and its type, GetInternalCacheEntry returns the internal cache entry for this object.
func (h *cmEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
	return nil
}

// Given an object key and its type, getResourceFromInformerCache returns the latest state of the object
// from the informers cache.
func (h *cmEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	var obj interface{}
	var name string
	var err error

	_, name, err = cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to split key %s: %v", key, err)
	}

	switch h.objType {
	case factory.NodeType,
		factory.EgressNodeType:
		obj, err = h.cm.watchFactory.GetNode(name)

	default:
		err = fmt.Errorf("object type %s not supported, cannot retrieve it from informers cache",
			h.objType)
	}
	return obj, err
}

// Given an object and its type, RecordAddEvent records the add event on this object.
func (h *cmEventHandler) RecordAddEvent(obj interface{}) {
}

// Given an object and its type, RecordUpdateEvent records the update event on this object.
func (h *cmEventHandler) RecordUpdateEvent(obj interface{}) {
}

// Given an object and its type, RecordDeleteEvent records the delete event on this object. Only used for pods now.
func (h *cmEventHandler) RecordDeleteEvent(obj interface{}) {
}

func (h *cmEventHandler) RecordSuccessEvent(obj interface{}) {
}

// Given an object and its type, RecordErrorEvent records an error event on this object.
// Only used for pods now.
func (h *cmEventHandler) RecordErrorEvent(obj interface{}, reason string, err error) {
}

// Given an object and its type, isResourceScheduled returns true if the object has been scheduled.
func (h *cmEventHandler) IsResourceScheduled(obj interface{}) bool {
	return true
}

// Given an object and its type, IsObjectInTerminalState returns true if the object is a in terminal state.
func (h *cmEventHandler) IsObjectInTerminalState(obj interface{}) bool {
	return false
}

// Given a *RetryFramework instance, an object to add and a boolean specifying if the function was executed from
// iterateRetryResources, AddResource adds the specified object to the cluster according to its type and
// returns the error, if any, yielded during object creation.
func (h *cmEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	var err error

	switch h.objType {
	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
		}
		if err = h.cm.addUpdateNodeEvent(node); err != nil {
			klog.Infof("Node add failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}
	default:
		return fmt.Errorf("no add function for object type %s", h.objType)
	}
	return nil
}

// Given a *RetryFramework instance, an old and a new object, UpdateResource updates the specified object in the cluster
// to its version in newObj according to its type and returns the error, if any, yielded during the object update.
// The inRetryCache boolean argument is to indicate if the given resource is in the retryCache or not.
func (h *cmEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	var err error

	switch h.objType {
	case factory.NodeType:
		node, ok := newObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", newObj)
		}
		if err = h.cm.addUpdateNodeEvent(node); err != nil {
			klog.Infof("Node update failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}
	default:
		return fmt.Errorf("no update function for object type %s", h.objType)
	}
	return nil
}

// Given a *RetryFramework instance, an object and optionally a cachedObj, DeleteResource deletes the object from the cluster
// according to the delete logic of its resource type. cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *cmEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	switch h.objType {
	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
		}
		return h.cm.deleteNode(node)
	}
	return nil
}

func (h *cmEventHandler) SyncFunc(objs []interface{}) error {
	var syncFunc func([]interface{}) error

	if h.syncFunc != nil {
		// syncFunc was provided explicitly
		syncFunc = h.syncFunc
	} else {
		switch h.objType {
		case factory.NodeType:
			syncFunc = h.cm.syncNodes

		default:
			return fmt.Errorf("no sync function for object type %s", h.objType)
		}
	}
	if syncFunc == nil {
		return nil
	}
	return syncFunc(objs)
}
