package clustermanager

import (
	"fmt"
	"sync"

	bitmapallocator "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/ipallocator/allocator"
)

// IDAllocator is used to allocate id for a resource and store the resource - id in a map
type IDAllocator struct {
	nameIdMap sync.Map
	idBitmap  *bitmapallocator.AllocationBitmap
	sync.Mutex
}

// NewIDAllocator returns an IDAllocator
func NewIDAllocator(name string, maxIds int) (*IDAllocator, error) {
	idBitmap := bitmapallocator.NewContiguousAllocationMap(maxIds, name)

	return &IDAllocator{
		nameIdMap: sync.Map{},
		idBitmap:  idBitmap,
	}, nil
}

// GetAllocatedID returns allocated ID for the resource 'name' if it is
// already allocated.  Else it returns -1 as the 'id' with an error.
func (idAllocator *IDAllocator) GetAllocatedID(name string) (int, error) {
	idAllocator.Lock()
	defer idAllocator.Unlock()

	// Check the idMap and return the id if its already allocated
	v, ok := idAllocator.nameIdMap.Load(name)
	if ok {
		return v.(int), nil
	}

	return -1, fmt.Errorf("id for the resource %s not found", name)
}

// AllocateId allocates an id for the resource 'name' and returns the id.
// If the id for the resource is already allocated, it returns the cached id.
func (idAllocator *IDAllocator) AllocateID(name string) (int, error) {
	idAllocator.Lock()
	defer idAllocator.Unlock()

	// Check the idMap and return the id if its already allocated
	v, ok := idAllocator.nameIdMap.Load(name)
	if ok {
		return v.(int), nil
	}

	id, allocated, _ := idAllocator.idBitmap.AllocateNext()

	if !allocated {
		return -1, fmt.Errorf("failed to allocate the id for the resource %s", name)
	}

	idAllocator.nameIdMap.Store(name, id)
	return id, nil
}

// ReserveID reserves the id 'id' for the resource 'name'. It returns an
// error if the 'id' is already reserved by a resource other than 'name'.
// It also returns an error if the resource 'name' has a different 'id'
// already reserved.
func (idAllocator *IDAllocator) ReserveID(name string, id int) error {
	idAllocator.Lock()
	defer idAllocator.Unlock()

	v, ok := idAllocator.nameIdMap.Load(name)
	if ok {
		if v.(int) == id {
			// All good. The id is already reserved by the same resource name.
			return nil
		}
		return fmt.Errorf("can't reserve id %d for the resource %s. It is already allocated with a different id %d", id, name, v.(int))
	}

	reserved, _ := idAllocator.idBitmap.Allocate(id)
	if !reserved {
		return fmt.Errorf("id %d is already reserved by another resource", id)
	}

	idAllocator.nameIdMap.Store(name, id)
	return nil
}

// ReleaseID releases the id allocated for the resource 'name'
func (idAllocator *IDAllocator) ReleaseID(name string) {
	idAllocator.Lock()
	defer idAllocator.Unlock()

	v, ok := idAllocator.nameIdMap.Load(name)
	if ok {
		idAllocator.idBitmap.Release(v.(int))
		idAllocator.nameIdMap.Delete(name)
	}
}
